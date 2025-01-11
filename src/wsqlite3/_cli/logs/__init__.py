import logging
import logging.handlers
import pathlib
from time import strftime, localtime


DEFAULT_FORMATTER = logging.Formatter('{asctime} {levelname} {threadName}/{label}: {message}', style="{", defaults={"label": ""})

FILEHANDLER_maxBytes = 10485760  # 80MiB (~10MB)
FILEHANDLER_backupCount = 1


def default_logger(name):
    _logger = logging.Logger(name)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(DEFAULT_FORMATTER)
    _logger.addHandler(stream_handler)
    return _logger


def default_filehandler(filepath: str):
    filehandler = logging.handlers.RotatingFileHandler(filepath, maxBytes=FILEHANDLER_maxBytes, backupCount=FILEHANDLER_backupCount)
    filehandler.setFormatter(DEFAULT_FORMATTER)
    return filehandler


cli_logfile_cache = pathlib.Path(__file__).parent


def cli_make_logfilename(name: str = ""):
    return "wsqlite3" + ("-" + name if name else "") + ".log"


def cli_make_logfilepath(name: str = ""):
    return cli_logfile_cache / cli_make_logfilename(name)


def cli_dump_logfile(name: str = ""):
    with open(cli_make_logfilepath(name)) as f:
        print(f.read())


def cli_open_logfile(name: str = ""):
    import subprocess
    import os
    import sys
    filepath = cli_make_logfilepath(name)
    if sys.platform == "darwin":  # macOS
        subprocess.call(('open', filepath))
    elif "win" in sys.platform:  # Windows Windows/Cygwin
        os.startfile(filepath)
    else:  # unix / linux / ...
        subprocess.call(('xdg-open', filepath))


def cli_list_logfiles():
    import os

    def sizeof_fmt(num):
        for unit in ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB"):
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}"
            num /= 1024.0
        return f"{num:.1f}YiB"

    entries = list(os.scandir(cli_logfile_cache))
    max_name_len = len(max(entries, key=lambda e: len(e.name)).name)
    sep_len = max_name_len + 28
    print("=" * sep_len)
    name = "%%-%ds" % max_name_len
    name %= "FILENAME"
    print(f"{name} {'ACCESS TIME':<14} {'FILE SIZE':<10}")
    print("=" * sep_len)
    for entry in entries:
        if entry.name.startswith("wsqlite3"):
            name = "%%-%ds" % max_name_len
            name %= entry.name
            stat = entry.stat()
            # max_name_len + 1 + 14                                            + 1 + ~10
            print(f"{name} {strftime('%y.%m.%d-%H:%M', localtime(stat.st_atime))} {sizeof_fmt(stat.st_size)}")
    print("-" * sep_len)

