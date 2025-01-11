from __future__ import annotations

try:
    import argparse
    argparse._ActionsContainer._check_conflict = lambda _, act: act
except Exception:
    raise
try:
    from .. import service, verbose_service, baseclient, __version__
    from .._cli import logs
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__
    from wsqlite3._cli import logs

PARSER = argparse.ArgumentParser(
    "wsqlite3",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
PARSER.description = """
The command line tool of wsqlite3 (https://srccircumflex.github.io/wsqlite3/)
"""
PARSER.epilog = ""


class _Dir:
    name: str | tuple[str, ...] = ""
    description: str = ""
    epilog: str = ""

    parser: argparse.ArgumentParser | argparse._ArgumentGroup

    def __init__(self, parser: argparse.ArgumentParser | argparse._ArgumentGroup = PARSER):
        self.parser = parser
        self.add_args()
        if isinstance(parser, argparse.ArgumentParser):
            self.make_usage()
            self.parser.description = self.description or parser.description
            self.parser.epilog = self.epilog or parser.epilog

    def add_args(self):
        ...

    @classmethod
    def as_group(cls, parser: argparse.ArgumentParser):
        return cls(parser.add_argument_group(str("|").join(cls.name), cls.description))

    def make_usage(self):
        usage = self.parser.format_usage()
        self.parser.usage = "wsqlite3 " + str("|").join(self.name) + usage[15:]

    def __eq__(self, other):
        return other == self.name


class DStart(_Dir):
    name = ("start", "on")
    description = "start the service"
    epilog = "a non detached process can be terminated by ctrl+c (depending on the version of asyncio a second SIGINT signal may be necessary)"

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--host", type=str, default="127.255.11.13", help="server host")
        self.parser.add_argument("--port", type=int, default=9998, help="server port")
        self.parser.add_argument("--threads", type=int, default=1, help="n threads")
        self.parser.add_argument("--cpt", type=int, default=0, help="connections per thread")
        self.parser.add_argument("--derivative", type=str, default=None, help="use the derived module under file path, must contain an object named <Server>", metavar="PATH")
        self.parser.add_argument("--debug", action="store_true", default=False, help="use <DebugServer> (ignored in combination with --derivative)")
        self.parser.add_argument("--verbose", action="store_true", default=False, help="use <VerboseServer> (ignored in combination with --derivative or --debug)")
        self.parser.add_argument("--logging", type=str, default=None, help="run: wsqlite3 logging --help", metavar="LOGLEVEL")
        self.parser.add_argument("--detach", action="store_true", default=False, help="detach the process")
        self.parser.add_argument("--prevent-autoclose", action="store_true", default=False, help="interrupt a running auto close process and make sure that the service is running (!does not interrupt an explicit auto close!)")
        self.parser.add_argument("--check-autoclose", type=int, default=None, help="trigger the autoclose function after the given seconds", metavar="SECONDS")
        self.parser.add_argument("--sigterm", action="store_true", default=False, help="send SIGTERM to the own process after completion of the main loop")


class DStop(_Dir):
    name = ("stop", "off")
    description = "stop the service under registry name and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--all", action="store_true", default=False, help="stop all registered services")
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--force", action="store_true", default=False, help="force shutdown")
        self.parser.add_argument("--commit", action="store_true", default=False, help="commit databases before shutdown")


class DPing(_Dir):
    name = ("ping",)
    description = "ping the service under registry name and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")


class DSend(_Dir):
    name = ("send",)
    description = "send json to service under registry name and exit"
    epilog = "autoclose is not triggered"

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("order", type=str, help="json data (plain)")


class DRegistry(_Dir):
    name = ("registry", "reg")
    description = "output the current service registry to stdout and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--all", action="store_true", help="")
        self.parser.add_argument("--force-flush", action="store_true", help="destroy the registry")
        self.parser.add_argument("--auto-flush", action="store_true", help="remove unreachable records from the registry")
        self.parser.add_argument("--get-file", action="store_true", help="get the registry file path")


class DLogging(_Dir):
    name = ("logging", "log")
    description = f"""
Logging to files is activated by the option --logging=LOGLEVEL from start.
The comprehensiveness can be extended by the combination with --verbose or --debug, 
respectively by --derivative=PATH which methods execute log itself
(if --logging is not combined, the logger is only used in fatal error handling).
LOGLEVEL can be a name or an integer (https://docs.python.org/3/library/logging.html#logging-levels).
Logfiles are located in `{logs.cli_logfile_cache}'.
Logfile names follow the scheme: `wsqlite3[-<registry-name>].log'.
A RotatingFileHandler is used (https://docs.python.org/3/library/logging.handlers.html#rotatingfilehandler), 
with the configuration (maxBytes=80MiB (~10MB), backupCount=1).
Changes to the configuration are possible only in the module ({logs.cli_logfile_cache}).
"""
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--path", action="store_true", help="show logfile path on stdout")
        self.parser.add_argument("--dump", action="store_true", help="dump logfile to stdout")
        self.parser.add_argument("--dir", action="store_true", help=f"list `{logs.cli_logfile_cache}'")
        self.parser.add_argument("--open", action="store_true", help="start a logfile with its associated application")


class DHelp(_Dir):
    name = ("help", "h")
    description = "show this help message and exit"
    epilog = " "

    def add_args(self):
        super().add_args()
        self.parser.add_argument("--doc", action="store_true", help="same as: wsqlite3 doc")


class DDocumentation(_Dir):
    name = ("documentation", "doc")
    description = "open https://srccircumflex.github.io/wsqlite3/ in the browser and exit"
    epilog = " "

    def add_args(self):
        super().add_args()


class DVersion(_Dir):
    name = ("version", "ver", "v")
    description = "output the version to stdout and exit"
    epilog = " "

    def add_args(self):
        super().add_args()


class HelpAll(_Dir):
    name = ""
    description = ""
    epilog = "use `wsqlite3 <command> --help' for selective help"

    def __init__(self):
        DStart.as_group(PARSER)
        DStop.as_group(PARSER)
        DPing.as_group(PARSER)
        DSend.as_group(PARSER)
        DRegistry.as_group(PARSER)
        DLogging.as_group(PARSER)
        DHelp.as_group(PARSER)
        DDocumentation.as_group(PARSER)
        DVersion.as_group(PARSER)
        _Dir.__init__(self)

    def make_usage(self):
        self.parser.usage = "wsqlite3 {start | stop | ping | send | registry | logging | help | doc | version} [options...]"
