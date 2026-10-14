import unittest
import sys
import threading
import time
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import public_logs_downloader as downloader  # noqa: E402


class AdaptiveLimiterTests(unittest.TestCase):
    def test_limiter_can_continue_after_reducing_concurrency(self):
        limiter = downloader.AdaptiveLimiter(
            initial=2,
            min_limit=1,
            max_limit=2,
            window=1,
        )

        assert limiter.acquire()
        limiter.release(success=False)

        assert limiter.acquire()
        limiter.release(success=True)


    def test_limiter_wait_is_cancelable(self):
        limiter = downloader.AdaptiveLimiter(initial=1, min_limit=1, max_limit=1)
        cancel_event = threading.Event()
        result = []

        assert limiter.acquire()
        waiter = threading.Thread(target=lambda: result.append(limiter.acquire(cancel_event)))
        waiter.start()
        time.sleep(0.05)
        cancel_event.set()
        waiter.join(timeout=1)
        limiter.release(success=True)

        assert not waiter.is_alive()
        assert result == [False]
