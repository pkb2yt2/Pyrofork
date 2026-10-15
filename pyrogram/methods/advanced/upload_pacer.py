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

"""Upload rate pacer. Backs off on FloodWait and creeps back up after clean parts."""

import asyncio
import time


class AdaptiveBucket:
    def __init__(self, rate, ceiling, burst=16, floor=1.0, step=1.0, backoff=0.85):
        self._rate = float(rate)
        self._ceiling = float(ceiling)
        self._burst = float(burst if burst is not None else rate)
        self._floor = float(floor)
        self._step = float(step)
        self._backoff = float(backoff)
        self._tokens = self._burst
        self._last_refill = time.monotonic()
        self._last_up = self._last_refill
        self._last_down = 0.0
        self._lock = asyncio.Lock()

    @property
    def rate(self):
        return self._rate

    @rate.setter
    def rate(self, value):
        self._refill()
        self._rate = float(value)

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self):
        # Sleep while holding the lock so waiters stay in arrival order.
        # Releasing the lock to sleep wakes every waiter for one token.
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                await asyncio.sleep((1.0 - self._tokens) / max(self._rate, 0.0001))

    def on_success(self):
        now = time.monotonic()
        if now - self._last_up >= 1.0 and now - self._last_down >= 2.0:
            self._last_up = now
            self.rate = min(self._rate + self._step, self._ceiling)

    def on_flood(self):
        now = time.monotonic()
        if now - self._last_down >= 1.0:
            self._last_down = now
            self._last_up = now
            self.rate = max(self._rate * self._backoff, self._floor)
