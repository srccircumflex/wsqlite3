from __future__ import annotations

import atexit
import importlib.util
import json
import socket as _socket
import sys
from typing import Type

try:
    from .. import service, verbose_service, baseclient, __version__
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__

try:
    import threading
    from threading import _shutdown as _shutdown_


    def _shutdown():
        try:
            return _shutdown_()
        except KeyboardInterrupt:
            pass


    threading._shutdown = _shutdown
except Exception:
    raise

from . import servreg
from . import argp


class _Exit(Exception):
    def __init__(self, code): self.code = code


def _get_from_reg(name, reg) -> list[str, int]:
    try:
        return reg[name]
    except KeyError:
        if name:
            print(repr(name), "not in registry", file=sys.stderr)
        else:
            print("default service not registered", file=sys.stderr)
        raise _Exit(1)


def _set_to_reg(name, reg, val):
    try:
        val = reg[name]
    except KeyError:
        reg[name] = val
    else:
        if name:
            print(repr(name), "set in registry:", val, file=sys.stderr)
        else:
            print("default service registered:", val, file=sys.stderr)
        raise _Exit(1)


class _ServiceConf:
    Reg: Type[servreg.ServiceReg] = servreg.ServiceReg
    Server: Type[service.Server] = service.Server
    host: str = "127.255.11.13"
    port: int = 9998
    threads: int = 1
    connections_per_thread: int = 0
    session_name: str = ""

    def start(self) -> service.Server:
        with self.Reg() as reg:
            _set_to_reg(self.session_name, reg, [self.host, self.port])
            atexit.register(self._flush)
            server = self.Server((self.host, self.port), self.threads, self.connections_per_thread)
            server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            server.socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
            server.start(wait_iterations=True)
            print("ws://%s:%d" % (self.host, self.port))
            return server

    def _flush(self):
        with self.Reg() as reg:
            reg.pop(self.session_name, None)


def _main():
    try:
        match _dir := sys.argv.pop(1):
            case argp.DStart.name:
                argp.DStart(argp.PARSER)
                args = argp.PARSER.parse_args()
                _service = _ServiceConf()
                _service.session_name = args.name
                _service.host = args.host
                _service.port = args.port
                _service.threads = args.threads
                _service.connections_per_thread = args.cpt
                if args.derivative:
                    spec = importlib.util.spec_from_file_location("", args.derivative)
                    service = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(service)
                    _service.Server = service.Server
                elif args.debug:
                    _service.Server = verbose_service.DebugServer
                elif args.verbose:
                    _service.Server = verbose_service.VerboseServer
                _service.start()
            case argp.DStop.name:
                argp.DStop(argp.PARSER)
                args = argp.PARSER.parse_args()
                with servreg.ServiceReg() as reg:
                    client = baseclient.Connection(*_get_from_reg(args.name, reg))
                    client.start()
                    with client:
                        client.order().server().shutdown(args.force, args.commit).__communicate__()
            case argp.DPing.name:
                argp.DPing(argp.PARSER)
                args = argp.PARSER.parse_args()
                with servreg.ServiceReg() as reg:
                    client = baseclient.Connection(*_get_from_reg(args.name, reg))
                    client.start()
                    with client:
                        pong = client.order().ping().__communicate__()
                        print(pong)
                        client.order().autoclose().value("skip!").__communicate__()
                        client.order().connection().destroy().__communicate__()
            case argp.DRegistry.name:
                argp.DRegistry(argp.PARSER)
                args = argp.PARSER.parse_args()
                if args.force_flush:
                    reg = servreg.ServiceReg()
                    reg._lock_release()
                    with reg as reg:
                        reg.clear()
                elif args.get_file:
                    print(servreg.ServiceReg.file)
                elif args.force_flush:
                    with servreg.ServiceReg() as reg:
                        reg.clear()
                elif args.all:
                    with servreg.ServiceReg() as reg:
                        print(json.dumps(reg, indent=4, sort_keys=True))
                else:
                    with servreg.ServiceReg() as reg:
                        print("%s:%d" % tuple(_get_from_reg(args.name, reg)))
            case argp.DVersion.name:
                argp.DVersion(argp.PARSER)
                argp.PARSER.parse_args()
                print(__version__)
            case _:
                raise IndexError
    except IndexError:
        sys.argv.insert(1, "-h")
        argp.HelpAll()
        argp.PARSER.parse_args()
    except _Exit as e:
        return e.code
    except KeyboardInterrupt:
        return 0

    return 0


def get_session_reg() -> dict[str, list[str, int]]:
    with servreg.ServiceReg() as reg:
        return reg
