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
    # Handshake Ping/InitConnection wait. Was 2s — under event-loop pressure
    # (Heroku R14 / concurrent work) that false-failed start() with TimeoutError
    # and left is_started clear forever when combined with an unbounded retry
    # loop. 5s is still far above a healthy DC reply (<1s) but survives brief
    # stalls. App-level session patches may raise this further for main only.
    START_TIMEOUT = 5
    WAIT_TIMEOUT = 15
    SLEEP_THRESHOLD = 10
    MAX_RETRIES = 10
    ACKS_THRESHOLD = 10
    PING_INTERVAL = 5
    STORED_MSG_IDS_MAX_SIZE = 500
    # Main-session start() attempt cap. Stock was `while True` forever — if the
    # DC was unreachable, restart() = stop()+start() never returned, which held
    # any single-flight restart flag forever and made every invoke fail with
    # "session restart in-flight". Media/CDN keep unbounded retry (slow upload
    # DC must not give up mid-transfer). None = unbounded.
    # Keep in sync with app session_patch soft budget when both are used;
    # media/CDN ignore this (unbounded). 4 is enough soft tries without
    # making one start() series longer than typical watchdog patience.
    START_MAX_ATTEMPTS_MAIN = 4
    # Bound stop() worker drains so a wedged recv/ping task cannot hang
    # restart() indefinitely (restart = stop + start).
    STOP_TASK_DRAIN_TIMEOUT = 8.0
    STOP_TASK_CANCEL_TIMEOUT = 3.0
    STOP_CONNECTION_CLOSE_TIMEOUT = 10.0

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
    # Heal-within-seconds tuning (2026-07-13). A healthy main-session Pong
    # returns in well under 1s, so a short probe timeout + low miss threshold
    # detects a wedge FAST without false-positives on a merely-slow-but-alive
    # session. Old values (10s probe × 3 misses on a 5s interval ≈ 30-45s to
    # restart) left the bot deaf for that whole window. New: 3s probe × 2 misses,
    # and once the FIRST miss happens the loop re-probes on a short backoff
    # (PING_WEDGE_BACKOFF) instead of the full 5s idle interval — so a real wedge
    # is confirmed + restart()ed in ~4-7s, not 30-45s. The interactive-reply
    # side is covered separately by the session-patch MAIN_WAIT_TIMEOUT (sends
    # fail fast so dispatcher workers aren't held) — this is the detection side.
    PING_PROBE_TIMEOUT = 3       # per-probe wait for the Pong (was 10)
    PING_HEALTH_MISSES = 2       # consecutive misses → restart (was 3)
    PING_WEDGE_BACKOFF = 1.0     # while mid-miss, re-probe this fast (not PING_INTERVAL)

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

        # Single-flight restart: waiters AWAIT completion (never fail-fast
        # "restart in-flight"). Process/dyno exit is the app's last resort —
        # session recovery stays in-process here.
        self._restart_lock = asyncio.Lock()
        self._restart_done = asyncio.Event()
        self._restart_done.set()  # no restart in progress at birth
        self._restart_generation = 0
        # Task currently running restart() critical section — send/invoke must
        # NOT wait on the lock if they are that task (handshake uses send;
        # waiting on the same lock = deadlock).
        self._restart_owner_task = None
        # Bound concurrent handle_updates so flood of pushes cannot starve
        # interactive RPCs (0s user-visible downtime goal).
        self._update_semaphore = asyncio.Semaphore(24)
        self._packet_tasks: set = set()
        self._last_restart_mono = 0.0
        self.MIN_RESTART_INTERVAL = 5.0

    async def start(self):
        # Main session: bound attempts so start() always returns (success or
        # ConnectionError). Unbounded while-True left restart() hanging forever
        # when Telegram/connect timed out, which is catastrophic for any
        # single-flight restart guard (invoke fails with "restart in-flight"
        # until the process dies). Media/CDN remain unbounded.
        _is_main = not (self.is_media or self.is_cdn)
        _max_attempts = self.START_MAX_ATTEMPTS_MAIN if _is_main else None
        _attempt = 0
        _last_err = None

        while True:
            _attempt += 1
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
            except (OSError, RPCError, TimeoutError, asyncio.TimeoutError) as e:
                # TimeoutError is a subclass of OSError on Py3.10+, but list
                # it explicitly so the intent is obvious and so a future
                # stdlib change cannot drop it from the soft-retry path.
                _last_err = e
                await self.stop()
                if _max_attempts is not None and _attempt >= _max_attempts:
                    raise ConnectionError(
                        f"start() failed after {_attempt} attempts "
                        f"(last error: {type(e).__name__}: {e})"
                    ) from e
                # Soft backoff — avoid hammering a flapping DC / thrashing dyno.
                _backoff = min(2 ** max(_attempt - 1, 0), 8)
                log.warning(
                    "Session start attempt %s/%s failed (%s: %s); "
                    "soft backoff %ss then retry.",
                    _attempt,
                    _max_attempts if _max_attempts is not None else "∞",
                    type(e).__name__, e, _backoff,
                )
                await asyncio.sleep(_backoff)
            except Exception as e:
                await self.stop()
                raise e
            else:
                break

        self.is_started.set()

        log.info("Session started")

    async def _drain_task(self, task, name: str) -> None:
        """Await a worker task with a timeout; force-cancel if it hangs.

        Used by stop() so a wedged recv_worker/ping_worker cannot block
        restart() forever (restart = await stop(); await start()).
        """
        if task is None:
            return
        try:
            await asyncio.wait_for(
                asyncio.shield(task),
                timeout=self.STOP_TASK_DRAIN_TIMEOUT,
            )
        except asyncio.TimeoutError:
            log.warning(
                "%s did not exit within %ss during stop() — force-cancelling.",
                name, self.STOP_TASK_DRAIN_TIMEOUT,
            )
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=self.STOP_TASK_CANCEL_TIMEOUT)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass
        except (asyncio.CancelledError, Exception):
            pass

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

        # Cancel in-flight packet handlers so they don't race the next start.
        for t in list(getattr(self, "_packet_tasks", set()) or set()):
            t.cancel()
        if getattr(self, "_packet_tasks", None):
            self._packet_tasks.clear()

        self.ping_task_event.set()

        await self._drain_task(self.ping_task, "ping_task")

        self.ping_task_event.clear()

        # Guard against stopping a session that never started (connection is None
        # until start() assigns it). Without this, stopping a not-yet-started
        # session — e.g. one sibling of a parallel multi-session upload whose
        # start() raised before connecting — throws AttributeError and leaks
        # the others. Makes stop() idempotent/safe on a cold session.
        if self.connection is not None:
            try:
                await asyncio.wait_for(
                    self.connection.close(),
                    timeout=self.STOP_CONNECTION_CLOSE_TIMEOUT,
                )
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception) as e:
                log.warning("connection.close() during stop() raised/timed out: %s", e)
            self.connection = None

        await self._drain_task(self.recv_task, "recv_task")

        if not self.is_media and callable(self.client.disconnect_handler):
            try:
                await self.client.disconnect_handler(self.client)
            except Exception as e:
                log.exception(e)

        log.info("Session stopped")

    def _i_own_restart_lock(self) -> bool:
        """True if the current task is the one running restart() critical section."""
        try:
            return asyncio.current_task() is self._restart_owner_task
        except Exception:
            return False

    async def _wait_if_restarting(self, timeout: Optional[float] = None) -> None:
        """External callers wait for restart; the restart owner must not (deadlock)."""
        if not self._restart_lock.locked() or self._i_own_restart_lock():
            return
        try:
            await asyncio.wait_for(
                self._restart_done.wait(),
                timeout=timeout if timeout is not None else self.WAIT_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise TimeoutError("Request timed out (waiting for session restart)")

    async def restart(self):
        """In-process transport recovery. Waiters block on the same lock until
        done — interactive invoke never needs a dyno/process restart for this.

        Soft reconnect under the lock (handshake uses send without waiting on
        that lock). Falls back to classic stop→start if soft path fails.
        """
        import time as _time

        # Nested call from the owner (e.g. invoke→restart while already in
        # restart) — no-op, avoid deadlock.
        if self._i_own_restart_lock():
            return

        # Already recovering — wait for it (wzgram-style). Do NOT raise.
        if self._restart_lock.locked():
            try:
                await asyncio.wait_for(self._restart_done.wait(), timeout=45.0)
            except asyncio.TimeoutError:
                log.warning("Session restart wait timed out (45s); continuing")
            return

        async with self._restart_lock:
            self._restart_owner_task = asyncio.current_task()
            self._restart_done.clear()
            try:
                now = _time.monotonic()
                # Anti-thrash ONLY when we have a live connection right after a
                # successful restart. Never skip if connection is missing (that
                # would leave the session permanently down for MIN interval).
                if (
                    self.is_started.is_set()
                    and self.connection is not None
                    and (now - self._last_restart_mono) < self.MIN_RESTART_INTERVAL
                ):
                    log.debug(
                        "Session restart skipped (min interval %.1fs, connection live)",
                        self.MIN_RESTART_INTERVAL,
                    )
                    return

                soft_ok = False
                if not (self.is_media or self.is_cdn):
                    try:
                        soft_ok = await self._restart_soft()
                    except Exception as soft_err:
                        log.warning(
                            "Soft session restart failed (%s: %s); "
                            "falling back to classic stop→start",
                            type(soft_err).__name__, soft_err,
                        )
                        soft_ok = False

                if not soft_ok:
                    await self.stop()
                    try:
                        await self.start()
                    except ConnectionError as e:
                        log.warning("Session restart failed: %s", e)

                self._last_restart_mono = _time.monotonic()
                self._restart_generation += 1
            finally:
                self._restart_owner_task = None
                self._restart_done.set()

    async def _restart_soft(self) -> bool:
        """Fast in-process reconnect under the restart lock.

        Tear down old transport → new connect + handshake → ping worker.
        Concurrent invoke/send wait on the lock (owner does not wait).
        Typical blackout ~0.3–2s when the DC is healthy — not a dyno recycle.

        Returns True on success. On exception, caller uses classic stop→start.
        """
        from pyrogram.raw.all import layer as _layer

        # Soft-stop transport without clearing is_started until new link works.
        self.stored_msg_ids.clear()
        for _result in list(self.results.values()):
            if not _result.event.is_set():
                _result.event.set()

        self.ping_task_event.set()
        await self._drain_task(self.ping_task, "ping_task")
        self.ping_task_event.clear()
        self.ping_task = None

        old_connection = self.connection
        self.connection = None
        await self._drain_task(self.recv_task, "recv_task")
        self.recv_task = None
        if old_connection is not None:
            try:
                await asyncio.wait_for(old_connection.close(), timeout=5.0)
            except Exception as e:
                log.debug("soft-restart close: %s", e)

        new_connection = self.client.connection_factory(
            dc_id=self.dc_id,
            test_mode=self.test_mode,
            ipv6=self.client.ipv6,
            alt_port=self.client.alt_port,
            proxy=self.client.proxy,
            media=self.is_media,
            protocol_factory=self.client.protocol_factory,
        )
        self.connection = new_connection
        try:
            await new_connection.connect()
            self.recv_task = self.loop.create_task(self.recv_worker())
            hs = max(float(self.START_TIMEOUT or 5), 6.0)
            # send() must not wait on restart lock — we own it (see _i_own_restart_lock).
            await self.send(raw.functions.Ping(ping_id=0), timeout=hs)
            if not self.is_cdn:
                await self.send(
                    raw.functions.InvokeWithLayer(
                        layer=_layer,
                        query=raw.functions.InitConnection(
                            api_id=await self.client.storage.api_id(),
                            app_version=self.client.app_version,
                            device_model=self.client.device_model,
                            system_version=self.client.system_version,
                            system_lang_code=self.client.system_lang_code,
                            lang_code=self.client.lang_code,
                            lang_pack=self.client.lang_pack,
                            query=raw.functions.help.GetConfig(),
                        ),
                    ),
                    timeout=hs,
                )
            self.ping_task = self.loop.create_task(self.ping_worker())
            self.is_started.set()
            log.info("Session restarted (soft / in-process, no dyno exit)")
            return True
        except Exception:
            try:
                await self.stop()
            except Exception:
                pass
            raise

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
                    self.loop.create_task(self._run_update(msg.body))

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

    async def _run_update(self, body):
        """Bound concurrent update handlers so floods cannot starve user RPCs."""
        try:
            async with self._update_semaphore:
                await self.client.handle_updates(body)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("handle_updates error suppressed: %s: %s", type(e).__name__, e)

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
            # Idle cadence is PING_INTERVAL; but once we've seen a miss (a wedge
            # may be starting) re-probe on the short PING_WEDGE_BACKOFF so 2
            # misses confirm in ~a few seconds instead of ~10s. Only applies to
            # the health-probing main session (health_misses stays 0 otherwise).
            _wait = self.PING_WEDGE_BACKOFF if (probe_health and health_misses > 0) else self.PING_INTERVAL
            try:
                await asyncio.wait_for(self.ping_task_event.wait(), _wait)
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
                # Lag-aware: if the event loop was blocked (heavy CPU/IO on
                # main), a short probe timeout is a false wedge — don't count.
                try:
                    import time as _t
                    _t0 = _t.monotonic()
                    await asyncio.sleep(0)
                    _lag = _t.monotonic() - _t0
                except Exception:
                    _lag = 0.0
                if _lag > 0.05:
                    log.debug(
                        "Keep-alive probe miss ignored (event-loop lag %.0fms)",
                        _lag * 1000,
                    )
                    continue

                health_misses += 1
                log.warning(
                    "Keep-alive probe got no reply (%s/%s) — session may be "
                    "wedged (socket alive, RPCs unanswered).",
                    health_misses, self.PING_HEALTH_MISSES
                )
                if health_misses >= self.PING_HEALTH_MISSES:
                    log.warning(
                        "Keep-alive starvation threshold reached — forcing "
                        "IN-PROCESS session restart (never dyno/process exit)."
                    )
                    self.loop.create_task(self.restart())
                    break
            except OSError:
                # Genuine socket-level failure — soft reconnect only.
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
                    # Awaitable path via create_task still ok; restart is
                    # single-flight and waiters block on the lock.
                    self.loop.create_task(self.restart())

                break

            if self.is_started.is_set():
                task = self.loop.create_task(self._handle_packet_tracked(packet))
                self._packet_tasks.add(task)
                task.add_done_callback(self._packet_tasks.discard)

        log.info("NetworkTask stopped")

    async def _handle_packet_tracked(self, packet):
        try:
            await self.handle_packet(packet)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.debug("handle_packet error: %s: %s", type(e).__name__, e)

    async def send(self, data: TLObject, wait_response: bool = True, timeout: float = WAIT_TIMEOUT, retry: int = 0):
        # External callers wait for restart; the restart owner must proceed
        # (handshake Ping/InitConnection) or we deadlock on the lock.
        await self._wait_if_restarting(timeout=timeout or self.WAIT_TIMEOUT)

        if self.connection is None:
            raise OSError("session connection is None (not started / mid-restart)")

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
        except AttributeError as e:
            self.results.pop(msg_id, None)
            raise OSError(f"session connection send race: {e}") from e

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
        if not self.stored_msg_ids:
            return
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
        # Wait for in-flight restart first (never fail-fast to callers).
        # Owner task skips wait (see _wait_if_restarting).
        try:
            await self._wait_if_restarting(timeout=timeout or self.WAIT_TIMEOUT)
        except TimeoutError:
            pass

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
            except (OSError, TimeoutError, asyncio.TimeoutError, InternalServerError, ServiceUnavailable) as e:
                # TimeoutError is OSError on 3.11+ — still list explicitly.
                if retries == 0:
                    raise e from None

                (log.warning if retries < 2 else log.info)(
                    '[%s] Retrying "%s" due to: %s',
                    Session.MAX_RETRIES - retries + 1,
                    query_name, str(e) or repr(e)
                )

                # Transport / timeout → soft in-process restart, never dyno exit.
                # Skip if we are already inside restart (nested invoke during handshake).
                if (
                    isinstance(e, (OSError, TimeoutError, asyncio.TimeoutError))
                    and not self._i_own_restart_lock()
                ):
                    try:
                        await self.restart()
                    except Exception as rst_err:
                        log.warning(
                            "invoke: restart after %s failed: %s",
                            type(e).__name__, rst_err,
                        )

                await asyncio.sleep(0.5)

                return await self.invoke(query, retries - 1, timeout)
