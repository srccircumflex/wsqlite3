from __future__ import annotations

try:
    import argparse
    argparse._ActionsContainer._check_conflict = lambda _, act: act
except Exception:
    raise
try:
    from .. import service, verbose_service, baseclient, __version__
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__

PARSER = argparse.ArgumentParser(
    "wsqlite3",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
)
PARSER.description = """
The command line tool of wsqlite3 (https://srccircumflex.github.io/wsqlite3/)
"""
PARSER.epilog = ""


class _Dir:
    name: str = ""
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
        return cls(parser.add_argument_group(cls.name, cls.description))

    def make_usage(self):
        usage = self.parser.format_usage()
        self.parser.usage = "wsqlite3 " + self.name + usage[15:]

    def __eq__(self, other):
        return other == self.name


class DStart(_Dir):
    name = "start"
    description = "start the service"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--host", type=str, default="127.255.11.13", help="server host")
        self.parser.add_argument("--port", type=int, default=9998, help="server port")
        self.parser.add_argument("--threads", type=int, default=1, help="n threads")
        self.parser.add_argument("--cpt", type=int, default=0, help="connections per thread")
        self.parser.add_argument("--derivative", type=str, default=None, help="use the derived module under file path, must contain an object named <Server>")
        self.parser.add_argument("--debug", action="store_true", default=False, help="use <DebugServer> (ignored in combination with --derivative)")
        self.parser.add_argument("--verbose", action="store_true", default=False, help="use <VerboseServer> (ignored in combination with --derivative or --debug)")


class DStop(_Dir):
    name = "stop"
    description = "stop the service under registry name and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--force", action="store_true", default=False, help="force shutdown")
        self.parser.add_argument("--commit", action="store_true", default=False, help="commit databases before shutdown")


class DPing(_Dir):
    name = "ping"
    description = "ping the service under registry name and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")


class DRegistry(_Dir):
    name = "registry"
    description = "output the current service registry to stdout and exit"
    epilog = " "

    def add_args(self):
        self.parser.add_argument("--name", type=str, default="", help="registry name")
        self.parser.add_argument("--all", action="store_true", help="")
        self.parser.add_argument("--force-flush", action="store_true", help="destroy the registry")
        self.parser.add_argument("--get-file", action="store_true", help="get the registry file path")


class DHelp(_Dir):
    name = "help"
    description = "show this help message and exit"
    epilog = " "

    def add_args(self):
        super().add_args()


class DVersion(_Dir):
    name = "version"
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
        DRegistry.as_group(PARSER)
        DHelp.as_group(PARSER)
        DVersion.as_group(PARSER)
        _Dir.__init__(self)

    def make_usage(self):
        self.parser.usage = "wsqlite3 {start | stop | ping | registry | help | version} [options...]"
