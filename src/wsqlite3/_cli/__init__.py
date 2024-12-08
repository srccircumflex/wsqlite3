from __future__ import annotations

import atexit
import importlib.util
import json
import socket as _socket
import subprocess
import sys
import time
from typing import Type

try:
    from .. import service, verbose_service, baseclient, __version__
    from . import servreg, argp
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__
    from wsqlite3._cli import servreg, argp

try:
    import threading
    from threading import _shutdown as _shutdown_


    def _shutdown():
        try:
            return _shutdown_()
        except KeyboardInterrupt:
            print()


    threading._shutdown = _shutdown
except Exception:
    raise


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

    prevent_autoclose: bool = False
    check_autoclose: float = False

    _detached: bool = False

    def _check_autoclose(self):
        time.sleep(0.2)
        time.sleep(self.check_autoclose)
        try:
            client = baseclient.Connection(self.host, self.port)
            client.start()
            with client:
                client.order().connection().destroy().__communicate__()
        except ConnectionRefusedError:
            pass

    def _prevent_autoclose(self):
        self.prevent_autoclose = False
        client = baseclient.Connection(self.host, self.port)
        try:
            client.start()
        except ConnectionRefusedError:
            self.start()
        try:
            with client:
                client.order().autoclose().value("skip!").cancel("force").__communicate__()
                client.order().connection().destroy().__communicate__()
        except ConnectionRefusedError:
            self.start()

    def start(self):
        try:
            with self.Reg() as reg:
                _set_to_reg(self.session_name, reg, [self.host, self.port])
                socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
                socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
                socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
                socket.bind((self.host, self.port))
                server = self.Server(socket, self.threads, self.connections_per_thread)
        except _Exit:
            if self.prevent_autoclose:
                self._prevent_autoclose()
            raise
        else:
            if self.check_autoclose:
                threading.Thread(target=self._check_autoclose).start()
            print("ws://%s:%d" % (self.host, self.port))
            try:
                server.run()
            finally:
                self._flush()

    def _flush(self):
        with self.Reg() as reg:
            reg.pop(self.session_name, None)


def _main():
    try:
        match _dir := sys.argv.pop(1):
            case argp.DStart.name:
                argp.DStart(argp.PARSER)
                args = argp.PARSER.parse_args()
                if args.detach:
                    sys.argv[0] = _dir
                    sys.argv.remove("--detach")
                    subprocess.Popen(
                        [sys.executable, __file__] + sys.argv,
                        stdout=sys.stdout, stderr=sys.stderr
                    )
                    return 0
                _service = _ServiceConf()
                _service.session_name = args.name
                _service.host = args.host
                _service.port = args.port
                _service.threads = args.threads
                _service.connections_per_thread = args.cpt
                _service.prevent_autoclose = args.prevent_autoclose
                _service.check_autoclose = args.check_autoclose
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
                        client.communicate([
                            client.order().autoclose().value("skip!"),
                            client.order().connection().destroy()
                        ])
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
                elif args.auto_flush:
                    session_reg_auto_flush()
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


def session_reg_auto_flush() -> dict[str, list[str, int]]:
    flush = dict()
    with servreg.ServiceReg() as reg:
        for name, addr in reg.copy().items():
            try:
                client = baseclient.Connection(*addr)
                client.start()
                with client:
                    client.communicate([
                        client.order().autoclose().value("skip!"),
                        client.order().connection().destroy()
                    ])
            except ConnectionRefusedError:
                flush[name] = reg.pop(name)
    return flush


if __name__ == '__main__':
    _ServiceConf._detached = True
    _main()
