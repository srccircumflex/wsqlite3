from __future__ import annotations

import socket
import sys
import argparse
import pathlib
import json
import os
import time
import atexit
from typing import Type

try:
    from . import _cli
except ImportError:
    from wsqlite3 import _cli


def run():
    exit(_cli._main())


if __name__ == "__main__":
    run()

