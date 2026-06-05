#  Pyrofork - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#  Copyright (C) 2022-present Mayuri-Chan <https://github.com/Mayuri-Chan>
#
#  This file is part of Pyrofork.
#
#  Pyrofork is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrofork is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrofork.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import bisect
import logging
import os
from hashlib import sha1
from io import BytesIO
from typing import Optional

import pyrogram
from pyrogram import raw
from pyrogram.connection import Connection
from pyrogram.crypto import mtproto
from pyrogram.errors import (
    RPCError, InternalServerError, AuthKeyDuplicated,
    FloodWait, FloodPremiumWait,
    ServiceUnavailable, BadMsgNotification,
    SecurityCheckMismatch, Unauthorized
)
from pyrogram.raw.all import layer
from pyrogram.raw.core import TLObject, MsgContainer, Int, FutureSalts
from .internals import MsgId, MsgFactory

log = logging.getLogger(__name__)


class Result:
    def __init__(self):
        self.value = None
        self.event = asyncio.Event()


class Session:
    START_TIMEOUT = 2
    WAIT_TIMEOUT = 15
    SLEEP_THRESHOLD = 10
    MAX_RETRIES = 10
    ACKS_THRESHOLD = 10
    PING_INTERVAL = 5
    STORED_MSG_IDS_MAX_SIZE = 500

    # ── Response-starvation recovery ────────────────────────────────────
    # The keep-alive ping below is normally fire-and-forget (wait_response
    # =False). That makes it blind to the "half-wedged" session: the TCP
    # socket is alive and recv_worker keeps reading bytes (so it never
    # returns None and never triggers restart()), but the server has
    # stopped answering our RPCs — every real send() waits out its full
    # timeout and raises TimeoutError, indefinitely, with no recovery.
    #
    # Fix: send the keep-alive WITH a response wait and treat it as a
    # liveness probe. A Pong reply (or even an RPCError) proves the
    # request/response path is alive and resets the counter. PING_HEALTH_
    # MISSES consecutive probes with no reply == the session is wedged →
    # trigger pyrofork's own in-process restart() (a clean ~10-15s
    # transport re-handshake; handlers/plugins/process all stay intact).
    # No os._exit, no dyno restart. Applied to the MAIN/auth session only;
    # media/CDN sessions keep the stock fire-and-forget ping (see ping_worker).
    PING_PROBE_TIMEOUT = 10      # per-probe wait for the Pong
    PING_HEALTH_MISSES = 3       # consecutive misses (~3×PING_INTERVAL ≈ 15s) → restart

    TRANSPORT_ERRORS = {
        404: "auth key not found",
        429: "transport flood",
        444: "invalid DC"
    }

    def __init__(
        self,
        client: "pyrogram.Client",
        dc_id: int,
        auth_key: bytes,
        test_mode: bool,
        is_media: bool = False,
        is_cdn: bool = False
    ):
        self.client = client
        self.dc_id = dc_id
        self.auth_key = auth_key
        self.test_mode = test_mode
        self.is_media = is_media
        self.is_cdn = is_cdn

        self.connection: Optional[Connection] = None

        self.auth_key_id = sha1(auth_key).digest()[-8:]

        self.session_id = os.urandom(8)
        self.msg_factory = MsgFactory()

        self.salt = 0

        self.pending_acks = set()

        self.results = {}

        self.stored_msg_ids = []

        self.ping_task = None
        self.ping_task_event = asyncio.Event()

        self.recv_task = None

        self.is_started = asyncio.Event()

        self.loop = asyncio.get_event_loop()

    async def start(self):
        while True:
            self.connection = self.client.connection_factory(
                dc_id=self.dc_id,
                test_mode=self.test_mode,
                ipv6=self.client.ipv6,
                alt_port=self.client.alt_port,
                proxy=self.client.proxy,
                media=self.is_media,
                protocol_factory=self.client.protocol_factory
            )

            try:
                await self.connection.connect()

                self.recv_task = self.loop.create_task(self.recv_worker())

                await self.send(raw.functions.Ping(ping_id=0), timeout=self.START_TIMEOUT)

                if not self.is_cdn:
                    await self.send(
                        raw.functions.InvokeWithLayer(
                            layer=layer,
                            query=raw.functions.InitConnection(
                                api_id=await self.client.storage.api_id(),
                                app_version=self.client.app_version,
                                device_model=self.client.device_model,
                                system_version=self.client.system_version,
                                system_lang_code=self.client.system_lang_code,
                                lang_code=self.client.lang_code,
                                lang_pack=self.client.lang_pack,
                                query=raw.functions.help.GetConfig(),
                            )
                        ),
                        timeout=self.START_TIMEOUT
                    )

                self.ping_task = self.loop.create_task(self.ping_worker())

                log.info("Session initialized: Layer %s", layer)
                log.info("Device: %s - %s", self.client.device_model, self.client.app_version)
                log.info("System: %s (%s)", self.client.system_version, self.client.lang_code)
            except AuthKeyDuplicated as e:
                await self.stop()
                raise e
            except (OSError, RPCError):
                await self.stop()
            except Exception as e:
                await self.stop()
                raise e
            else:
                break

        self.is_started.set()

        log.info("Session started")

    async def stop(self):
        self.is_started.clear()

        self.stored_msg_ids.clear()

        # Wake any send() coroutines that are blocked waiting on a response
        # that will never arrive on this (about-to-die) connection. Without
        # this, each one sits out its full WAIT_TIMEOUT after the restart
        # before raising TimeoutError. Setting the event lets them resume
        # now, pop a None value, and raise/retry against the fresh session
        # immediately. (The msg_ids are dead the moment the connection
        # closes, so there's nothing to lose by releasing them early.)
        for _result in list(self.results.values()):
            if not _result.event.is_set():
                _result.event.set()

        self.ping_task_event.set()

        if self.ping_task is not None:
            await self.ping_task

        self.ping_task_event.clear()

        await self.connection.close()

        if self.recv_task:
            await self.recv_task

        if not self.is_media and callable(self.client.disconnect_handler):
            try:
                await self.client.disconnect_handler(self.client)
            except Exception as e:
                log.exception(e)

        log.info("Session stopped")

    async def restart(self):
        await self.stop()
        await self.start()

    async def handle_packet(self, packet):
        try:
            data = await self.loop.run_in_executor(
                pyrogram.crypto_executor,
                mtproto.unpack,
                BytesIO(packet),
                self.session_id,
                self.auth_key,
                self.auth_key_id
            )
        except ValueError as e:
            log.debug(e)
            self.loop.create_task(self.restart())
            return

        messages = (
            data.body.messages
            if isinstance(data.body, MsgContainer)
            else [data]
        )

        log.debug("Received: %s", data)

        for msg in messages:
            if msg.seq_no % 2 != 0:
                if msg.msg_id in self.pending_acks:
                    continue
                else:
                    self.pending_acks.add(msg.msg_id)

            try:
                if len(self.stored_msg_ids) > Session.STORED_MSG_IDS_MAX_SIZE:
                    del self.stored_msg_ids[:Session.STORED_MSG_IDS_MAX_SIZE // 2]

                if self.stored_msg_ids:
                    if msg.msg_id < self.stored_msg_ids[0]:
                        raise SecurityCheckMismatch("The msg_id is lower than all the stored values")

                    if msg.msg_id in self.stored_msg_ids:
                        raise SecurityCheckMismatch("The msg_id is equal to any of the stored values")

                    time_diff = (msg.msg_id - MsgId()) / 2 ** 32

                    if time_diff > 30:
                        raise SecurityCheckMismatch("The msg_id belongs to over 30 seconds in the future. "
                                                    "Most likely the client time has to be synchronized.")

                    if time_diff < -300:
                        raise SecurityCheckMismatch("The msg_id belongs to over 300 seconds in the past. "
                                                    "Most likely the client time has to be synchronized.")
            except SecurityCheckMismatch as e:
                log.info("Discarding packet: %s", e)
                await self.connection.close()
                return
            else:
                bisect.insort(self.stored_msg_ids, msg.msg_id)

            if isinstance(msg.body, (raw.types.MsgDetailedInfo, raw.types.MsgNewDetailedInfo)):
                self.pending_acks.add(msg.body.answer_msg_id)
                continue

            if isinstance(msg.body, raw.types.NewSessionCreated):
                continue

            msg_id = None

            if isinstance(msg.body, (raw.types.BadMsgNotification, raw.types.BadServerSalt)):
                msg_id = msg.body.bad_msg_id
            elif isinstance(msg.body, (FutureSalts, raw.types.RpcResult)):
                msg_id = msg.body.req_msg_id
            elif isinstance(msg.body, raw.types.Pong):
                msg_id = msg.body.msg_id
            else:
                if self.client is not None:
                    self.loop.create_task(self.client.handle_updates(msg.body))

            if msg_id in self.results:
                self.results[msg_id].value = getattr(msg.body, "result", msg.body)
                self.results[msg_id].event.set()

        if len(self.pending_acks) >= self.ACKS_THRESHOLD:
            log.debug("Sending %s acks", len(self.pending_acks))

            try:
                await self.send(raw.types.MsgsAck(msg_ids=list(self.pending_acks)), False)
            except OSError:
                pass
            else:
                self.pending_acks.clear()

    async def ping_worker(self):
        log.info("PingTask started")

        # Only the MAIN/auth session (carries send_message, get_me, all the
        # text RPCs) runs the response-starvation probe. Media/CDN sessions
        # stream SaveBigFilePart/GetFile chunks where a control Pong can
        # legitimately queue behind chunk traffic — restarting one mid-
        # transfer would abort an upload/download. They keep the original
        # fire-and-forget keep-alive (zero behaviour change), and they're
        # already recovered on a genuinely dead socket by recv_worker. The
        # real-world wedge we're fixing was on the main session.
        probe_health = not (self.is_media or self.is_cdn)

        # Consecutive keep-alive probes that got no reply. Reset to 0 the
        # moment the server answers (Pong or even an RPCError — both prove
        # the request/response path is alive). See PING_HEALTH_MISSES.
        health_misses = 0

        while True:
            try:
                await asyncio.wait_for(self.ping_task_event.wait(), self.PING_INTERVAL)
            except asyncio.TimeoutError:
                pass
            else:
                break

            if not probe_health:
                # ── Original fire-and-forget keep-alive (media/CDN) ──
                try:
                    await self.send(
                        raw.functions.PingDelayDisconnect(
                            ping_id=0, disconnect_delay=self.WAIT_TIMEOUT + 10
                        ), False
                    )
                except OSError:
                    self.loop.create_task(self.restart())
                    break
                except RPCError:
                    pass
                continue

            # ── Main session: keep-alive doubles as a liveness probe ──
            # Only probe once the session is fully up. During start()/
            # restart() the InitConnection handshake is still in flight;
            # probing then could race and false-trigger a second restart.
            if not self.is_started.is_set():
                continue

            try:
                # Was wait_response=False (fire-and-forget). Now we WAIT for
                # the Pong so a wedged-but-connected session is detectable:
                # no Pong within PING_PROBE_TIMEOUT == a miss.
                await self.send(
                    raw.functions.PingDelayDisconnect(
                        ping_id=0, disconnect_delay=self.WAIT_TIMEOUT + 10
                    ),
                    timeout=self.PING_PROBE_TIMEOUT
                )
                # Server answered — path is healthy.
                health_misses = 0
            except (asyncio.TimeoutError, TimeoutError):
                # ORDER MATTERS: this clause MUST come before `except OSError`.
                # On Python 3.11+ (we run 3.12) the builtin TimeoutError — and
                # its alias asyncio.TimeoutError — is a SUBCLASS of OSError, so
                # a bare `except OSError` first would swallow the probe timeout
                # and we'd never count misses.
                #
                # No reply in time == the half-wedge signal: recv_worker is
                # still happily reading bytes (so it won't restart on its own),
                # but our RPCs are going unanswered. Count it; once we cross
                # the threshold, force the same in-process reconnect recv_worker
                # would do on a dead socket. (self.restart is wrapped single-
                # flight by the app's session patch, so concurrent restart
                # requests from recv_worker/handle_packet collapse to one.)
                health_misses += 1
                log.warning(
                    "Keep-alive probe got no reply (%s/%s) — session may be "
                    "wedged (socket alive, RPCs unanswered).",
                    health_misses, self.PING_HEALTH_MISSES
                )
                if health_misses >= self.PING_HEALTH_MISSES:
                    log.warning(
                        "Keep-alive starvation threshold reached — forcing "
                        "session restart (in-process, no process exit)."
                    )
                    self.loop.create_task(self.restart())
                    break
            except OSError:
                # Genuine socket-level failure (connection.send wraps socket
                # errors as OSError). The original behaviour — reconnect now.
                self.loop.create_task(self.restart())
                break
            except RPCError:
                # The server answered with an error — the request/response
                # path is alive, which is all this probe checks. Not a miss.
                health_misses = 0

        log.info("PingTask stopped")

    async def recv_worker(self):
        log.info("NetworkTask started")

        while True:
            packet = await self.connection.recv()

            if packet is None or len(packet) == 4:
                if packet:
                    error_code = -Int.read(BytesIO(packet))

                    if error_code == 404:
                        raise Unauthorized(
                            "Auth key not found in the system. You must delete your session file "
                            "and log in again with your phone number or bot token."
                        )

                    log.warning(
                        "Server sent transport error: %s (%s)",
                        error_code, Session.TRANSPORT_ERRORS.get(error_code, "unknown error")
                    )

                if self.is_started.is_set():
                    self.loop.create_task(self.restart())

                break

            self.loop.create_task(self.handle_packet(packet))

        log.info("NetworkTask stopped")

    async def send(self, data: TLObject, wait_response: bool = True, timeout: float = WAIT_TIMEOUT, retry: int = 0):
        message = self.msg_factory(data)
        msg_id = message.msg_id

        if wait_response:
            self.results[msg_id] = Result()

        log.debug("Sent: %s", message)

        payload = await self.loop.run_in_executor(
            pyrogram.crypto_executor,
            mtproto.pack,
            message,
            self.salt,
            self.session_id,
            self.auth_key,
            self.auth_key_id
        )

        try:
            await self.connection.send(payload)
        except OSError as e:
            self.results.pop(msg_id, None)
            raise e

        if wait_response:
            try:
                await asyncio.wait_for(self.results[msg_id].event.wait(), timeout)
            except asyncio.TimeoutError:
                pass

            result = self.results.pop(msg_id).value

            if result is None:
                raise TimeoutError("Request timed out")

            if isinstance(result, raw.types.RpcError):
                if isinstance(data, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
                    data = data.query

                RPCError.raise_it(result, type(data))

            if isinstance(result, raw.types.BadMsgNotification):
                if retry > 1:
                    raise BadMsgNotification(result.error_code)

                self._handle_bad_notification()
                await self.send(data, wait_response, timeout, retry + 1)

            if isinstance(result, raw.types.BadServerSalt):
                self.salt = result.new_server_salt
                return await self.send(data, wait_response, timeout)

            return result

    def _handle_bad_notification(self):
        new_msg_id = MsgId()
        if self.stored_msg_ids[len(self.stored_msg_ids)-1] >= new_msg_id:
            new_msg_id = self.stored_msg_ids[len(self.stored_msg_ids)-1] + 4
            log.debug("Changing msg_id old=%s new=%s", self.stored_msg_ids[len(self.stored_msg_ids)-1], new_msg_id)
        self.stored_msg_ids[len(self.stored_msg_ids)-1] = new_msg_id

    async def invoke(
        self,
        query: TLObject,
        retries: int = MAX_RETRIES,
        timeout: float = WAIT_TIMEOUT,
        sleep_threshold: float = SLEEP_THRESHOLD
    ):
        try:
            await asyncio.wait_for(self.is_started.wait(), self.WAIT_TIMEOUT)
        except asyncio.TimeoutError:
            pass

        if isinstance(query, (raw.functions.InvokeWithoutUpdates, raw.functions.InvokeWithTakeout)):
            inner_query = query.query
        else:
            inner_query = query

        query_name = ".".join(inner_query.QUALNAME.split(".")[1:])

        while True:
            try:
                return await self.send(query, timeout=timeout)
            except (FloodWait, FloodPremiumWait) as e:
                amount = e.value

                if amount > sleep_threshold >= 0:
                    raise

                log.warning('[%s] Waiting for %s seconds before continuing (required by "%s")',
                            self.client.name, amount, query_name)

                await asyncio.sleep(amount)
            except (OSError, InternalServerError, ServiceUnavailable) as e:
                if retries == 0:
                    raise e from None

                (log.warning if retries < 2 else log.info)(
                    '[%s] Retrying "%s" due to: %s',
                    Session.MAX_RETRIES - retries + 1,
                    query_name, str(e) or repr(e)
                )

                await asyncio.sleep(0.5)

                return await self.invoke(query, retries - 1, timeout)
