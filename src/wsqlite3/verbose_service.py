from __future__ import annotations
from typing import Callable, Any, Literal

import asyncio
import json
import socket as _socket
import time
import traceback

import wsdatautil

from .service import Server, Connection, Operator, ConnectionsThread, Database


def log(server: Server, label: str, *message: object):
    print("", end="", flush=True)
    print(f"[{time.time():,.8f}] ({server.address[0]}:{server.address[1]}/{label})", *message, sep="\n  ", flush=True)


class VerboseOperator(Operator):

    async def read_orders(self, order_root: dict, res_order: dict):
        log(self.server, self.current_connection.id, "order:", json.dumps(order_root, sort_keys=True, indent=2))
        return await super().read_orders(order_root, res_order)

    def exception_message_formatter(self, exc: Exception) -> Any:
        log(self.server, self.current_connection.id, "order error:", str().join(traceback.format_exception(exc)))
        msg = super().exception_message_formatter(exc)
        log(self.server, self.current_connection.id, "order exception message:", json.dumps(msg, sort_keys=True, indent=2))
        return msg


class VerboseConnection(Connection):

    def __init__(self, server: Server, thread: ConnectionsThread | None, sock: _socket.socket, addr: tuple[str, int]):
        super().__init__(server, thread, sock, addr)
        log(self.server, "%s:%d" % addr, "connection accepted:", "uuid =", self.id)

    async def ws_wait_handshake(self) -> wsdatautil.HandshakeRequest:
        hs = await super().ws_wait_handshake()
        log(self.server, self.id, "ws.handshake:", hs)
        return hs

    async def read_one_frame(self) -> wsdatautil.Frame:
        fr = await super().read_one_frame()
        log(self.server, self.id, "ws.frame:", fr)
        return fr

    async def read_iteration(self):
        log(self.server, self.id, "await input")
        return await super().read_iteration()

    def destroy(self, force: bool = False, _skip_conn_locks: set[Connection] = ()) -> bool:
        log(self.server, self.id, "destroy")
        return super().destroy(force=force, _skip_conn_locks=_skip_conn_locks)

    async def at_connection_broken(self, exc: asyncio.IncompleteReadError) -> None:
        log(self.server, self.id, "connection broken:", str().join(traceback.format_exception(exc)))
        return await super().at_connection_broken(exc)

    async def at_unexpected_error(self, exc: Exception) -> bool:
        log(self.server, self.id, "unexpected error:", str().join(traceback.format_exception(exc)))
        return await super().at_unexpected_error(exc)


class VerboseConnectionsThread(ConnectionsThread):

    def run(self) -> None:
        super().run()
        log(self.server, f"t:{self.id}", "leave main loop")


class VerboseServer(Server):

    def __init__(
            self,
            socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = VerboseConnection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = VerboseConnectionsThread,
            factory_Operator: Callable[..., Operator] = VerboseOperator,
            factory_Database: Callable[..., Database] = Database,
    ):
        super().__init__(socket, threads, connections_per_thread, factory_Connection, factory_ConnectionsThread, factory_Operator, factory_Database)

    async def connection_request(self, sock: _socket.socket, addr: tuple[str, int]):
        log(self, "%s:%d" % addr, "connection request")
        return await super().connection_request(sock, addr)

    async def autoclose(self, from_: Connection, trigger: Connection, reason: str):
        log(self, trigger.id, f"autoclose: {from_=} {trigger=} {reason=}")
        return await super().autoclose(from_, trigger, reason)


class DebugOperator(VerboseOperator):

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

    async def proc_order__ping(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"ping":', order)
        err = await super().proc_order__ping(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order___exec(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"_exec":', order)
        err = await super().proc_order___exec(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order___getattr(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"_getattr":', order)
        err = await super().proc_order___getattr(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__connection(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"connection":', order)
        err = await super().proc_order__connection(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__server(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"server":', order)
        err = await super().proc_order__server(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__thread(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"thread":', order)
        err = await super().proc_order__thread(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__sql(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"db":', order)
        err = await super().proc_order__sql(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__broadcast(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"broadcast":', order)
        err = await super().proc_order__broadcast(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err

    async def proc_order__autoclose(self, order: dict, res: dict):
        log(self.server, self.current_connection.id, '"autoclose":', order)
        err = await super().proc_order__autoclose(order, res)
        order.pop("error", None)
        if order:
            log(self.server, self.current_connection.id, "unprocessed instructions:", order)
            self.erract_unprocessed_fields()
        return err


class DebugServer(VerboseServer):

    def __init__(
            self,
            socket: tuple[str, int] | _socket.socket,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = VerboseConnection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = VerboseConnectionsThread,
            factory_Operator: Callable[..., Operator] = DebugOperator,
            factory_Database: Callable[..., Database] = Database,
    ):
        if not isinstance(socket, _socket.socket):
            address = socket
            socket = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            socket.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEPORT, 1)
            socket.bind(address)
        super().__init__(socket, threads, connections_per_thread, factory_Connection, factory_ConnectionsThread, factory_Operator, factory_Database)

