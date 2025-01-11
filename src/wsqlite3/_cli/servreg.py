from __future__ import annotations

import json
import os
import pathlib
import time

try:
    from .. import service, verbose_service, baseclient, __version__
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__


LIB_ROOT = pathlib.Path(__file__).parent


class ServiceReg:

    file: pathlib.Path = LIB_ROOT / "service-sessions.json"
    lock: pathlib.Path = LIB_ROOT / "service-sessions.lock"
    json: dict[str, list[int, str, int]]

    wait_lock_timeout: tuple[int, float] = (1000, .001)

    def _lock_acquire(self):
        open(self.lock, "w").close()

    def _lock_release(self):
        try:
            os.remove(self.lock)
            return True
        except FileNotFoundError:
            return False

    def _dump(self):
        with open(self.file, "w") as f:
            json.dump(self.json, f)
        return self.json

    def _load(self):
        with open(self.file) as f:
            self.json = json.load(f)
        return self.json

    def __enter__(self) -> dict[str, list[int, str, int]]:
        iterations, timeout = self.wait_lock_timeout
        for i in range(iterations):
            if self.lock.exists():
                time.sleep(timeout)
            else:
                break
        else:
            raise TimeoutError
        try:
            self._lock_acquire()
            try:
                return self._load()
            except FileNotFoundError:
                self.json = dict()
                return self._dump()
        except Exception:
            self._lock_release()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._dump()
        self._lock_release()
