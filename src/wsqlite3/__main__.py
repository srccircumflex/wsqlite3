from __future__ import annotations

try:
    from . import _cli
except ImportError:
    from wsqlite3 import _cli


def run():
    exit(_cli._main())


if __name__ == "__main__":
    run()

