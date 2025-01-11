from __future__ import annotations

import asyncio
import json
import logging
import socket as _socket
import traceback
from typing import Callable, Any, Literal

import wsdatautil

from . import ServerConfig
from .service import Server, Connection, Operator, ConnectionsThread, Database


def log(server: Server, lv: int, label: str, message: str): server.logger.log(lv, message, extra={"label": label})


class LogOperator(Operator):

    async def read_orders(self, order_root: dict, res_order: dict):
        log(self.server, logging.INFO, self.current_connection.id, "order: " + json.dumps(order_root, sort_keys=True, indent=2))
        return await super().read_orders(order_root, res_order)

    def exception_message_formatter(self, exc: Exception) -> Any:
        msg = super().exception_message_formatter(exc)
        log(self.server, logging.ERROR, self.current_connection.id, "order exception message: " + json.dumps(msg, sort_keys=True, indent=2))
        return msg


class LogConnection(Connection):

    def __init__(self, server: Server, thread: ConnectionsThread | None, sock: _socket.socket, addr: tuple[str, int], id: str):
        super().__init__(server, thread, sock, addr, id)
        log(self.server, logging.INFO, "%s:%d" % addr, "connection accepted: uid = " + self.id)

    async def ws_wait_handshake(self) -> wsdatautil.HandshakeRequest:
        hs = await super().ws_wait_handshake()
        log(self.server, logging.INFO, self.id, "ws.handshake: " + repr(hs))
        return hs

    async def read_one_frame(self) -> wsdatautil.Frame:
        fr = await super().read_one_frame()
        log(self.server, logging.INFO, self.id, "ws.frame: " + repr(fr))
        return fr

    async def read_iteration(self):
        log(self.server, logging.INFO, self.id, "await input")
        return await super().read_iteration()

    def destroy(self, force: bool = False, _skip_conn_locks: set[Connection] = ()) -> bool:
        log(self.server, logging.INFO, self.id, "destroy")
        return super().destroy(force=force, _skip_conn_locks=_skip_conn_locks)

    async def at_connection_broken(self, exc: asyncio.IncompleteReadError) -> None:
        log(self.server, logging.WARNING, self.id, f"connection broken: {self} partial read: {exc.partial} ({len(exc.partial)}/{exc.expected})")
        return await super().at_connection_broken(exc)

    async def at_unexpected_error(self, exc: Exception) -> bool:
        log(self.server, logging.CRITICAL, self.id, "unexpected error: " + str().join(traceback.format_exception(exc)))
        return await super().at_unexpected_error(exc)


class LogConnectionsThread(ConnectionsThread):

    def run(self) -> None:
        super().run()
        log(self.server, logging.INFO, ".", "leave loop (connections-thread)")


class LogServer(Server):
    def __init__(
            self, socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = LogConnection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = LogConnectionsThread,
            factory_Operator: Callable[..., Operator] = LogOperator,
            factory_Database: Callable[..., Database] = Database,
            config: ServerConfig = ServerConfig(),
            logger: logging.Logger | None = None
    ):
        if logger is None:
            from ._cli.logs import default_logger
            logger = default_logger(self.__class__.__qualname__)
        super().__init__(socket, threads, connections_per_thread, factory_Connection, factory_ConnectionsThread, factory_Operator, factory_Database, config, logger)

    def connection_request(self, sock: _socket.socket, addr: tuple[str, int]):
        log(self, logging.INFO, "%s:%d" % addr, "connection request")
        return super().connection_request(sock, addr)

    async def autoclose(self, from_: Connection, trigger: Connection, reason: str):
        log(self, logging.INFO, trigger.id, f"autoclose: {from_=} {trigger=} {reason=}")
        return await super().autoclose(from_, trigger, reason)

    def _cleanup(self, *_):
        log(self, logging.INFO, ".", "cleanup...")
        super()._cleanup(*_)


class VerboseServer(LogServer):

    def __init__(
            self, socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = LogConnection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = LogConnectionsThread,
            factory_Operator: Callable[..., Operator] = LogOperator,
            factory_Database: Callable[..., Database] = Database,
            config: ServerConfig = ServerConfig(),
            logger: logging.Logger | None = None
    ):
        super().__init__(socket, threads, connections_per_thread, factory_Connection, factory_ConnectionsThread, factory_Operator, factory_Database, config, logger)


class DebugOperator(LogOperator):

    def __init__(
            self,
            connection: Connection,
            erract_unprocessed_fields: Literal[
                                           "cancel order",
                                           "cancel session",
                                           "ignore",
                                           "destroy connection",
                                           "shutdown server",
                                           "force shutdown server",
                                           "... +rollback"
                                       ] | str = "shutdown server"
    ):
        super().__init__(connection)

        rollback = "+rollback" in erract_unprocessed_fields
        erract_unprocessed_fields = self.error_actions[erract_unprocessed_fields]

        def __erract_unprocessed_fields():
            erract_unprocessed_fields(rollback)

        self.erract_unprocessed_fields = __erract_unprocessed_fields

    def exception_message_formatter(self, exc: Exception) -> Any:
        log(self.server, 5, self.current_connection.id, "order error: " + str().join(traceback.format_exception(exc)))
        return super().exception_message_formatter(exc)

    def _log(self, k: str, order: dict):
        log(self.server, logging.DEBUG, self.current_connection.id, f"'{k}': " + json.dumps(order, sort_keys=True, indent=2))

    async def proc_order__ping(self, order: dict, res: dict):
        self._log("ping", order)
        err = await super().proc_order__ping(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order___exec(self, order: dict, res: dict):
        self._log("_exec", order)
        err = await super().proc_order___exec(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order___getattr(self, order: dict, res: dict):
        self._log("_getattr", order)
        err = await super().proc_order___getattr(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__connection(self, order: dict, res: dict):
        self._log("connection", order)
        err = await super().proc_order__connection(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__server(self, order: dict, res: dict):
        self._log("server", order)
        err = await super().proc_order__server(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__thread(self, order: dict, res: dict):
        self._log("thread", order)
        err = await super().proc_order__thread(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__sql(self, order: dict, res: dict):
        self._log("db", order)
        err = await super().proc_order__sql(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__broadcast(self, order: dict, res: dict):
        self._log("broadcast", order)
        err = await super().proc_order__broadcast(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__autoclose(self, order: dict, res: dict):
        self._log("autoclose", order)
        err = await super().proc_order__autoclose(order, res)
        order.pop("error", None)
        if order:
            self._log("unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err


class DebugServer(VerboseServer):

    def __init__(
            self, socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = LogConnection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = LogConnectionsThread,
            factory_Operator: Callable[..., Operator] = DebugOperator,
            factory_Database: Callable[..., Database] = Database,
            config: ServerConfig = ServerConfig(),
            logger: logging.Logger | None = None
    ):
        if not isinstance(socket, _socket.socket):
            address = socket
            socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
            socket.bind(address)
        super().__init__(socket, threads, connections_per_thread, factory_Connection, factory_ConnectionsThread, factory_Operator, factory_Database, config, logger)
