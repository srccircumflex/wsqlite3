from __future__ import annotations

import atexit
import errno
import importlib.util
import json
import logging
import os
import signal
import socket as _socket
import subprocess
import sys
import time
import webbrowser
from typing import Type

try:
    from .. import service, verbose_service, baseclient, __version__
    from . import servreg, argp, logs
except ImportError:
    from wsqlite3 import service, verbose_service, baseclient, __version__
    from wsqlite3._cli import servreg, argp, logs

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


def _get_from_reg(name, reg) -> list[int, str, int]:
    try:
        return reg[name]
    except KeyError:
        if name:
            print(repr(name), "not in registry", file=sys.stderr)
        else:
            print("default service not registered", file=sys.stderr)
        raise _Exit(1)


def _set_to_reg(name, reg, val: list[int, str, int]):
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


def _cli_logging(lv, msg):
    ...


class _ServiceConf:
    Reg: Type[servreg.ServiceReg] = servreg.ServiceReg
    Server: Type[service.Server] = service.Server
    host: str = "127.255.11.13"
    port: int = 9998
    threads: int = 1
    connections_per_thread: int = 0
    session_name: str = ""

    prevent_autoclose: bool = False
    check_autoclose: int = False

    sigterm: bool = False

    logging: int | str | None = None
    verbose: bool = False

    _detached: bool = False

    def _check_autoclose(self):
        time.sleep(self.check_autoclose % 10)
        for _ in range(self.check_autoclose // 10):
            time.sleep(10)
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

    def _make_logger(self, server: service.Server):
        if self.logging is not None:
            _logger = server.logger or logs.default_logger(self.session_name)
            filehandler = logs.default_filehandler(logs.cli_make_logfilepath(self.session_name))
            _logger.addHandler(filehandler)
            log_lvs = {  # https://docs.python.org/3/library/logging.html#logging-levels
                "NOTSET": 0,
                "DEBUG": 10,
                "INFO": 20,
                "WARNINGS": 30,
                "ERROR": 40,
                "CRITICAL": 50,
            }
            self.logging = log_lvs.get(self.logging.upper(), self.logging)
            try:
                self.logging = int(self.logging)
            except ValueError:
                print(f"ValueError: invalid logger level: {self.logging}", file=sys.stderr)
                print(f"Chose a level name or custom int literal:", file=sys.stderr)
                for k, v in log_lvs.items():
                    print(f"{k:<9}: {v}", file=sys.stderr)
                raise _Exit(1)
            filehandler.setLevel(self.logging)
            server.logger = _logger

    def start(self):
        global _cli_logging

        socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
        socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
        socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
        socket.bind((self.host, self.port))

        try:
            server = self.Server(socket, self.threads, self.connections_per_thread)

            self._make_logger(server)
            if server.logger is not None:
                def _cli_logging(lv, msg): verbose_service.log(server, lv, "cli" + (f"/{self.session_name}" if self.session_name else ""), msg)
                _cli_logging = _cli_logging

            try:
                with self.Reg() as reg:
                    entry = [os.getpid(), self.host, self.port]
                    _cli_logging(logging.DEBUG, f"create registry entry: [(pid=){entry[0]}, (host=){self.host}, (port=){self.port}]")
                    _set_to_reg(self.session_name, reg, entry)
                    atexit.register(self._flush)
            except _Exit:
                if self.prevent_autoclose:
                    self._prevent_autoclose()
                raise
            else:
                if self.check_autoclose:
                    threading.Thread(target=self._check_autoclose, daemon=True).start()

                ws_url = "ws://%s:%d" % (self.host, self.port)
                print(ws_url)
                _cli_logging(logging.INFO, ("detached " if self._detached else "") + f"start @ {ws_url}")
                try:
                    server.run()
                finally:
                    _cli_logging(logging.DEBUG, "server closed")
                    if self.sigterm:
                        self._flush()
                        self._kill()
        finally:
            socket.close()

    def _flush(self):
        with self.Reg() as reg:
            reg.pop(self.session_name, None)
        _cli_logging(logging.DEBUG, "registry flushed")

    def _kill(self):
        pid = os.getpid()
        _cli_logging(logging.DEBUG, f"SIGTERM -> {pid} ...")
        os.kill(os.getpid(), signal.SIGTERM)


def _main():
    try:
        _dir = sys.argv.pop(1)
        if _dir in argp.DStart.name:
            argp.DStart(argp.PARSER)
            args = argp.PARSER.parse_args()
            if args.detach:
                sys.argv[0] = _dir
                sys.argv.remove("--detach")
                p = subprocess.Popen(
                    [sys.executable, __file__] + sys.argv,
                    stderr=subprocess.PIPE,
                    text=True
                )
                print(p.pid, flush=True)
                try:
                    print(p.communicate(timeout=.2)[1], end="", file=sys.stderr, flush=True)
                    return 1
                except subprocess.TimeoutExpired:
                    return 0
            else:
                _service = _ServiceConf()
                _service.session_name = args.name
                _service.host = args.host
                _service.port = args.port
                _service.threads = args.threads
                _service.connections_per_thread = args.cpt
                _service.prevent_autoclose = args.prevent_autoclose
                _service.check_autoclose = args.check_autoclose
                _service.sigterm = args.sigterm
                _service.logging = args.logging
                _service.verbose = args.verbose
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
        elif _dir in argp.DStop.name:
            argp.DStop(argp.PARSER)
            args = argp.PARSER.parse_args()
            if args.all:
                for reg in get_session_reg().values():
                    client = baseclient.Connection(*reg[1:])
                    client.start()
                    with client:
                        client.order().server().shutdown(args.force, args.commit).__communicate__()
            else:
                with servreg.ServiceReg() as reg:
                    client = baseclient.Connection(*_get_from_reg(args.name, reg)[1:])
                    client.start()
                    with client:
                        client.order().server().shutdown(args.force, args.commit).__communicate__()
        elif _dir in argp.DPing.name:
            argp.DPing(argp.PARSER)
            args = argp.PARSER.parse_args()
            with servreg.ServiceReg() as reg:
                client = baseclient.Connection(*_get_from_reg(args.name, reg)[1:])
                client.start()
                with client:
                    pong = client.order().ping().__communicate__()
                    print(json.dumps(pong, sort_keys=True, indent=2))
                    client.communicate([
                        client.order().autoclose().value("skip!"),
                        client.order().connection().destroy()
                    ])
        elif _dir in argp.DSend.name:
            argp.DSend(argp.PARSER)
            args = argp.PARSER.parse_args()
            order = json.loads(args.order)
            with servreg.ServiceReg() as reg:
                client = baseclient.Connection(*_get_from_reg(args.name, reg)[1:])
                client.start()
                with client:
                    client.send_obj(order)
                    resp = client.recv_obj(timeout=1)
                    print(json.dumps(resp, sort_keys=True, indent=2))
                    try:
                        client.communicate([
                            client.order().autoclose().value("skip!"),
                            client.order().connection().destroy()
                        ])
                    except OSError as e:
                        if e.errno != errno.EBADF:  # Bad file descriptor
                            raise
        elif _dir in argp.DRegistry.name:
            argp.DRegistry(argp.PARSER)
            args = argp.PARSER.parse_args()
            if args.force_flush:
                reg = servreg.ServiceReg()
                reg._lock_release()
                with reg as reg:
                    reg.clear()
            elif args.get_file:
                print(servreg.ServiceReg.file)
            elif args.auto_flush:
                session_reg_auto_flush()
            elif args.all:
                with servreg.ServiceReg() as reg:
                    print(json.dumps(reg, indent=4, sort_keys=True))
            else:
                with servreg.ServiceReg() as reg:
                    print("%s:%d" % tuple(_get_from_reg(args.name, reg)[1:]))
        elif _dir in argp.DLogging.name:
            argp.DLogging(argp.PARSER)
            args = argp.PARSER.parse_args()
            if args.dump:
                try:
                    logs.cli_dump_logfile(args.name)
                except FileNotFoundError as e:
                    print(e)
                    raise _Exit(1)
            if args.path:
                print(logs.cli_make_logfilepath(args.name))
            elif args.dir:
                logs.cli_list_logfiles()
            elif args.open:
                try:
                    logs.cli_open_logfile(args.name)
                except Exception as e:
                    print(e)
                    raise _Exit(1)
            else:
                argp.PARSER.print_help()
                argp.PARSER.exit()
        elif _dir in argp.DHelp.name:
            argp.DHelp(argp.PARSER)
            if argp.PARSER.parse_args().doc:
                webbrowser.open_new_tab("https://srccircumflex.github.io/wsqlite3/")
            else:
                raise IndexError
        elif _dir in argp.DDocumentation.name:
            argp.DDocumentation(argp.PARSER)
            webbrowser.open_new_tab("https://srccircumflex.github.io/wsqlite3/")
        elif _dir in argp.DVersion.name:
            argp.DVersion(argp.PARSER)
            argp.PARSER.parse_args()
            print(__version__)
        elif _dir in argp.DVersion.name:
            argp.DVersion(argp.PARSER)
            argp.PARSER.parse_args()
            print(__version__)
        else:
            raise IndexError
    except IndexError:
        sys.argv.insert(1, "-h")
        argp.HelpAll()
        argp.PARSER.parse_args()
    except _Exit as e:
        return e.code
    except KeyboardInterrupt:
        return 0
    else:
        return 0
    finally:
        _cli_logging(logging.INFO, "exit")


def get_session_reg() -> dict[str, list[int, str, int]]:
    with servreg.ServiceReg() as reg:
        return reg


def session_reg_auto_flush() -> dict[str, list[int, str, int]]:
    flush = dict()
    with servreg.ServiceReg() as reg:
        for name, addr in reg.copy().items():
            try:
                client = baseclient.Connection(*addr[1:])
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
