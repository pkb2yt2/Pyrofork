import asyncio
import os
import unittest

from pyrogram.methods.advanced.save_file import upload_pool_size
from pyrogram.methods.advanced.upload_pacer import AdaptiveBucket


class UploadPoolSizeTest(unittest.TestCase):
    def test_small_part_count_does_not_open_extra_sockets(self):
        self.assertEqual(upload_pool_size(False, True, 1), 1)
        self.assertEqual(upload_pool_size(False, True, 2), 1)

    def test_premium_uses_the_wide_pool(self):
        self.assertEqual(upload_pool_size(False, True, 10_000), 14)

    def test_bot_stays_smaller_than_premium(self):
        self.assertEqual(upload_pool_size(True, True, 10_000), 8)
        self.assertLess(
            upload_pool_size(True, False, 10_000),
            upload_pool_size(False, True, 10_000),
        )

    def test_env_ceiling(self):
        old = os.environ.get("PYROFORK_UPLOAD_MAX_SESSIONS")
        os.environ["PYROFORK_UPLOAD_MAX_SESSIONS"] = "3"
        try:
            self.assertEqual(upload_pool_size(False, True, 10_000), 3)
        finally:
            if old is None:
                os.environ.pop("PYROFORK_UPLOAD_MAX_SESSIONS", None)
            else:
                os.environ["PYROFORK_UPLOAD_MAX_SESSIONS"] = old


class AdaptiveBucketTest(unittest.TestCase):
    def test_flood_backs_off_and_success_climbs(self):
        bucket = AdaptiveBucket(rate=32, ceiling=40, burst=4, step=1.0, backoff=0.5)

        async def run():
            await bucket.acquire()
            bucket.on_flood()
            self.assertLess(bucket.rate, 32)
            slowed = bucket.rate
            bucket._last_up = 0
            bucket._last_down = 0
            bucket.on_success()
            self.assertGreater(bucket.rate, slowed)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
