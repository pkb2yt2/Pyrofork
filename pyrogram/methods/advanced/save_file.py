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

import asyncio
import functools
import inspect
import io
import logging
import math
import os
import time
from hashlib import md5
from pathlib import PurePath
from typing import Union, BinaryIO, Callable

import pyrogram
from pyrogram import StopTransmission
from pyrogram import raw
from pyrogram.errors import (
    RPCError, FloodWait, FloodPremiumWait,
    InternalServerError, ServiceUnavailable,
)
from pyrogram.methods.advanced.upload_pacer import AdaptiveBucket
from pyrogram.session import Session

log = logging.getLogger(__name__)

PART_SIZE = 512 * 1024
POOL_CAP = 20
WORKERS_PER_SESSION = 2
# One media connection must not carry more than this many upload RPCs.
# Past it, every request on that socket times out together.
INFLIGHT_PER_SESSION = 6
MAX_RETRIES = 16
STALL_TIMEOUT = 900
MAX_BATCH = 4 * 1024 * 1024
PACER_BURST = 16
POOL_IDLE_SECONDS = 300
MEDIA_PART_TIMEOUT = 60


def upload_pool_size(is_bot, is_premium, parts_left):
    """How many media connections this upload should use.

    Premium accounts get the wide pool. Bots and normal users stay smaller
    because their flood budget is lower. ``PYROFORK_UPLOAD_MAX_SESSIONS`` is
    still an optional ceiling.
    """
    parts_left = max(1, int(parts_left or 1))
    # One connection can usefully feed two workers, so don't open more
    # sockets than half the remaining parts.
    part_cap = max(1, math.ceil(parts_left / WORKERS_PER_SESSION))

    def _env_int(name, default):
        try:
            return int(os.environ.get(name, "") or default)
        except ValueError:
            return default

    if is_bot:
        want = _env_int("PYROFORK_UPLOAD_POOL_BOT", 8)
    elif is_premium:
        want = _env_int("PYROFORK_UPLOAD_POOL_PREMIUM", 14)
    else:
        want = _env_int("PYROFORK_UPLOAD_POOL_USER", 12)

    ceiling = _env_int("PYROFORK_UPLOAD_MAX_SESSIONS", POOL_CAP)
    return max(1, min(want, ceiling, POOL_CAP, part_cap))


def _inflight(session):
    sem = getattr(session, "_upload_inflight", None)
    if sem is None:
        sem = asyncio.Semaphore(INFLIGHT_PER_SESSION)
        session._upload_inflight = sem
    return sem


async def _stop_quiet(session):
    try:
        await session.stop()
    except Exception:
        log.debug("upload pool: stop failed", exc_info=True)


async def _stop_workers(queue, workers):
    """Deliver one sentinel per worker that is still running.

    The queue is bounded by the worker count. Putting a sentinel after the
    workers have already exited waits forever, and save_file_semaphore stays
    held for every later upload.
    """
    delivered = 0
    for _ in workers:
        if all(t.done() for t in workers):
            break
        try:
            await asyncio.wait_for(queue.put(None), MEDIA_PART_TIMEOUT)
        except asyncio.TimeoutError:
            break
        delivered += 1

    if delivered < len(workers):
        for task in workers:
            if not task.done():
                task.cancel()

    return await asyncio.gather(*workers, return_exceptions=True)


def _raise_worker_errors(results):
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            raise result


class SaveFile:
    async def _acquire_upload_sessions(self, dc_id, count):
        """Reuse media sessions on this DC. Downloads do not use this pool."""
        pools = self.upload_session_pools
        lock = self.upload_session_pools_lock
        async with lock:
            pool = pools.setdefault(dc_id, [])
            now = time.monotonic()
            kept = []
            for session in pool:
                in_use = getattr(session, "_upload_users", 0)
                started = session.is_started.is_set()
                idle_for = now - getattr(session, "_upload_last_used", now)
                if not started and in_use == 0:
                    await _stop_quiet(session)
                    continue
                if in_use == 0 and idle_for > POOL_IDLE_SECONDS and len(kept) >= count:
                    await _stop_quiet(session)
                    continue
                kept.append(session)
            pool[:] = kept

            needed = count - len(pool)
            if needed > 0:
                auth_key = await self.storage.auth_key()
                test_mode = await self.storage.test_mode()
                while needed > 0:
                    chunk = min(needed, 3)
                    created = await asyncio.gather(*(
                        self._start_upload_session(dc_id, auth_key, test_mode)
                        for _ in range(chunk)
                    ), return_exceptions=True)
                    ok = [s for s in created if not isinstance(s, BaseException)]
                    failed = [s for s in created if isinstance(s, BaseException)]
                    pool.extend(ok)
                    if failed:
                        if not pool:
                            raise failed[0]
                        log.warning(
                            "Upload pool for DC %s is short by %d session(s): %s",
                            dc_id, needed - len(ok), failed[0],
                        )
                        break
                    needed -= chunk

            chosen = list(pool[:count])
            for session in chosen:
                session._upload_users = getattr(session, "_upload_users", 0) + 1
                session._upload_last_used = now
            return chosen

    async def _start_upload_session(self, dc_id, auth_key, test_mode):
        session = Session(self, dc_id, auth_key, test_mode, is_media=True)
        try:
            await session.start()
        except BaseException:
            await _stop_quiet(session)
            raise
        return session

    async def _release_upload_sessions(self, sessions):
        if not sessions:
            return
        lock = getattr(self, "upload_session_pools_lock", None)
        if lock is None:
            return
        async with lock:
            now = time.monotonic()
            for session in sessions:
                session._upload_users = max(0, getattr(session, "_upload_users", 1) - 1)
                session._upload_last_used = now

    async def save_file(
        self: "pyrogram.Client",
        path: Union[str, BinaryIO],
        file_id: int = None,
        file_part: int = 0,
        progress: Callable = None,
        progress_args: tuple = ()
    ):
        """Upload a file onto Telegram servers, without actually sending the message to anyone.
        Useful whenever an InputFile type is required.

        .. note::

            This is a utility method intended to be used **only** when working with raw
            :obj:`functions <pyrogram.api.functions>` (i.e: a Telegram API method you wish to use which is not
            available yet in the Client class as an easy-to-use method).

        .. include:: /_includes/usable-by/users-bots.rst

        Parameters:
            path (``str`` | ``BinaryIO``):
                The path of the file you want to upload that exists on your local machine or a binary file-like object
                with its attribute ".name" set for in-memory uploads.

            file_id (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            file_part (``int``, *optional*):
                In case a file part expired, pass the file_id and the file_part to retry uploading that specific chunk.

            progress (``Callable``, *optional*):
                Pass a callback function to view the file transmission progress.
                The function must take *(current, total)* as positional arguments (look at Other Parameters below for a
                detailed description) and will be called back each time a new file chunk has been successfully
                transmitted.

            progress_args (``tuple``, *optional*):
                Extra custom arguments for the progress callback function.
                You can pass anything you need to be available in the progress callback scope; for example, a Message
                object or a Client instance in order to edit the message with the updated progress status.

        Other Parameters:
            current (``int``):
                The amount of bytes transmitted so far.

            total (``int``):
                The total size of the file.

            *args (``tuple``, *optional*):
                Extra custom arguments as defined in the ``progress_args`` parameter.
                You can either keep ``*args`` or add every single extra argument in your function signature.

        Returns:
            ``InputFile``: On success, the uploaded file is returned in form of an InputFile object.

        Raises:
            RPCError: In case of a Telegram RPC error.
        """
        async with self.save_file_semaphore:
            if path is None:
                return None

            part_size = PART_SIZE
            opened_here = False
            sessions = []
            workers = []
            queue = None
            next_batch_task = None
            fp = None

            if isinstance(path, (str, PurePath)):
                fp = open(path, "rb")
                opened_here = True
            elif isinstance(path, io.IOBase):
                fp = path
            else:
                raise ValueError(
                    "Invalid file. Expected a file path as string or a binary (not text) file pointer"
                )

            file_name = getattr(fp, "name", "file.jpg")

            try:
                fp.seek(0, os.SEEK_END)
                file_size = fp.tell()
                fp.seek(0)

                if file_size == 0:
                    raise ValueError("File size equals to 0 B")

                is_bot = bool(getattr(self.me, "is_bot", False))
                is_premium = bool(getattr(self.me, "is_premium", False))
                file_size_limit_mib = 4000 if is_premium else 2000

                if file_size > file_size_limit_mib * 1024 * 1024:
                    raise ValueError(
                        f"Can't upload files bigger than {file_size_limit_mib} MiB"
                    )

                file_total_parts = int(math.ceil(file_size / part_size))
                is_big = file_size > 10 * 1024 * 1024
                is_missing_part = file_id is not None
                file_id = file_id or self.rnd_id()
                md5_sum = md5() if not is_big and not is_missing_part else None

                parts_left = 1 if is_missing_part else max(1, file_total_parts - file_part)
                pool_size = 1 if is_missing_part or not is_big else upload_pool_size(
                    is_bot, is_premium, parts_left,
                )

                dc_id = await self.storage.dc_id()
                sessions = await self._acquire_upload_sessions(dc_id, pool_size)
                if not sessions:
                    raise OSError(f"No upload session available for DC {dc_id}")

                n_workers = min(
                    len(sessions) * WORKERS_PER_SESSION,
                    len(sessions) * INFLIGHT_PER_SESSION,
                    parts_left,
                )
                n_workers = max(1, n_workers)
                queue = asyncio.Queue(n_workers)
                acked = [0]
                stalled_since = [0.0]

                if is_bot:
                    rate = _env_rate("PYROFORK_UPLOAD_RATE_BOT", 32)
                elif is_premium:
                    rate = _env_rate("PYROFORK_UPLOAD_RATE_PREMIUM", 300)
                else:
                    rate = _env_rate("PYROFORK_UPLOAD_RATE_USER", 32)
                pacer = AdaptiveBucket(rate=rate, ceiling=max(rate, 300), burst=PACER_BURST, step=1.0)
                paced = is_bot or not is_premium

                async def _send_part(session, data):
                    for attempt in range(MAX_RETRIES):
                        try:
                            accepted = await session.invoke(
                                data,
                                timeout=MEDIA_PART_TIMEOUT,
                                sleep_threshold=0,
                            )
                            if not accepted:
                                raise OSError("part not accepted by the server")
                            return
                        except StopTransmission:
                            raise
                        except (FloodWait, FloodPremiumWait) as exc:
                            if attempt == MAX_RETRIES - 1:
                                raise
                            delay = exc.value if isinstance(exc.value, int) else 1
                            delay = min(max(delay, 1), 300)
                            pacer.on_flood()
                            log.warning(
                                "Retrying upload part after flood (attempt %d/%d, %ss)",
                                attempt + 1, MAX_RETRIES, delay,
                            )
                            await asyncio.sleep(delay)
                        except (OSError, TimeoutError, asyncio.TimeoutError,
                                InternalServerError, ServiceUnavailable) as exc:
                            if attempt == MAX_RETRIES - 1:
                                log.exception("Upload part failed after %d attempts", MAX_RETRIES)
                                raise
                            delay = min(2 ** attempt, 30)
                            log.warning(
                                "Retrying upload part (attempt %d/%d): %s",
                                attempt + 1, MAX_RETRIES, str(exc)[:120],
                            )
                            await asyncio.sleep(delay)
                        except RPCError as exc:
                            code = exc.CODE or 0
                            if 500 <= code < 600 and attempt < MAX_RETRIES - 1:
                                await asyncio.sleep(min(2 ** attempt, 30))
                                continue
                            raise

                async def worker(session):
                    slots = _inflight(session)
                    while True:
                        data = await queue.get()
                        if data is None:
                            return
                        try:
                            async with slots:
                                await _send_part(session, data)
                            acked[0] += 1
                            stalled_since[0] = 0.0
                            if paced:
                                pacer.on_success()
                        finally:
                            data = None

                async def read_batch():
                    batch_size = min(part_size * n_workers, MAX_BATCH)
                    return await self.loop.run_in_executor(self.executor, fp.read, batch_size)

                async def check_workers():
                    for task in workers:
                        if task.done() and not task.cancelled():
                            exc = task.exception()
                            if exc is not None:
                                raise exc

                last_reported = [-1]

                async def report(parts):
                    if not progress or parts == last_reported[0]:
                        return
                    last_reported[0] = parts
                    func = functools.partial(
                        progress,
                        min(parts * part_size, file_size),
                        file_size,
                        *progress_args,
                    )
                    try:
                        if inspect.iscoroutinefunction(progress):
                            await func()
                        else:
                            await self.loop.run_in_executor(self.executor, func)
                    except StopTransmission:
                        raise
                    except Exception as exc:
                        log.warning("Upload progress callback error: %s", exc)

                workers = [
                    self.loop.create_task(worker(sessions[i % len(sessions)]))
                    for i in range(n_workers)
                ]

                fp.seek(part_size * file_part)
                next_batch_task = self.loop.create_task(read_batch())

                while True:
                    batch = await next_batch_task
                    next_batch_task = self.loop.create_task(read_batch())

                    if not batch:
                        next_batch_task.cancel()
                        next_batch_task = None
                        if md5_sum is not None:
                            md5_sum = md5_sum.hexdigest()
                        break

                    await check_workers()

                    for start in range(0, len(batch), part_size):
                        chunk = batch[start:start + part_size]
                        if is_big:
                            rpc = raw.functions.upload.SaveBigFilePart(
                                file_id=file_id,
                                file_part=file_part,
                                file_total_parts=file_total_parts,
                                bytes=chunk,
                            )
                        else:
                            rpc = raw.functions.upload.SaveFilePart(
                                file_id=file_id,
                                file_part=file_part,
                                bytes=chunk,
                            )

                        if paced:
                            await pacer.acquire()

                        while True:
                            # A dead worker never drains the queue. Wake as soon
                            # as one exits, instead of waiting out the put timeout.
                            put_task = asyncio.ensure_future(queue.put(rpc))
                            live = [t for t in workers if not t.done()]
                            done, _pending = await asyncio.wait(
                                [put_task, *live],
                                timeout=30,
                                return_when=asyncio.FIRST_COMPLETED,
                            )
                            if put_task in done and not put_task.cancelled():
                                put_exc = put_task.exception()
                                if put_exc is not None:
                                    raise put_exc
                                await check_workers()
                                break
                            if not put_task.done():
                                put_task.cancel()
                            await check_workers()
                            if not done:
                                now = time.monotonic()
                                if stalled_since[0] == 0.0:
                                    stalled_since[0] = now
                                    log.warning(
                                        "Upload queue full: workers throttled, waiting up to %ss",
                                        STALL_TIMEOUT,
                                    )
                                elif now - stalled_since[0] > STALL_TIMEOUT:
                                    raise TimeoutError(
                                        f"Upload stalled: no part completed for {STALL_TIMEOUT}s"
                                    )

                        if is_missing_part:
                            if next_batch_task is not None and not next_batch_task.done():
                                next_batch_task.cancel()
                            _raise_worker_errors(await _stop_workers(queue, workers))
                            workers = []
                            return None

                        if md5_sum is not None:
                            md5_sum.update(chunk)

                        file_part += 1
                        await report(acked[0])

                    batch = None

                _raise_worker_errors(await _stop_workers(queue, workers))
                workers = []
                await report(file_total_parts)

                if is_big:
                    return raw.types.InputFileBig(
                        id=file_id,
                        parts=file_total_parts,
                        name=file_name,
                    )
                return raw.types.InputFile(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,
                    md5_checksum=md5_sum,
                )
            except StopTransmission:
                raise
            finally:
                if next_batch_task is not None and not next_batch_task.done():
                    next_batch_task.cancel()
                if workers and queue is not None:
                    try:
                        await _stop_workers(queue, workers)
                    except Exception:
                        log.debug("upload: worker shutdown failed", exc_info=True)
                await self._release_upload_sessions(sessions)
                if opened_here and fp is not None:
                    fp.close()


def _env_rate(name, default):
    try:
        return int(os.environ.get(name, "") or default)
    except ValueError:
        return default
