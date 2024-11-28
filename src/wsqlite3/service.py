from __future__ import annotations
from typing import Callable, Coroutine, Any, Literal, overload, Type

import asyncio
import json
import pickle
import socket
import sqlite3
import threading
from time import sleep, asctime, time, time_ns
from warnings import filterwarnings
from collections import OrderedDict
from collections.abc import Sequence, Mapping, Iterable, Hashable
from os import PathLike
from traceback import format_exception
from uuid import uuid4
from base64 import b64encode, b64decode

import wsdatautil


def _FATAL_ERROR_HANDLE(server: Server, label: str, exc: Exception, msg: str = ""):
    print("", end="", flush=True)
    print(f"""\x1b[1m\x1b[31m>>>\x1b[0m
{str("").join(format_exception(exc))}
\x1b[1m[{asctime()}] ({server.address[0]}:{server.address[1]}/{label}) {msg}
\x1b[31m<<<\x1b[0m""", flush=True)


def _MAKE_SQLITE3_PARAMS(params: tuple[tuple, dict]) -> tuple[tuple, dict]:
    """Since the signature of ``sqlite3.connect`` can change with the python versions (especially 3.12 and 3.15),
    the parameterization is not defined in detail and positional parameters are converted into keyword parameters
    before they are passed to ``sqlite3.connect``. In addition, the parameter `check_same_thread` is automatically
    set to ``False``.
    """
    args, kwargs = params
    for key, arg in zip(
            (
                    "database",
                    "timeout",
                    "detect_types",
                    "isolation_level",
                    "check_same_thread",
                    "factory",
                    "cached_statements",
                    "uri",
            ),
            args
    ):
        kwargs[key] = arg
    kwargs["check_same_thread"] = False
    return (), kwargs


class WSQLite3Error(Exception):
    params: Any

    def __init__(self, msg, params: Any = None):
        Exception.__init__(self, msg)
        self.params = params


class CursorLockedError(WSQLite3Error):
    ...


class CursorNotLockedError(WSQLite3Error):
    ...


class OrderError(WSQLite3Error):
    ...


class ConfigurationError(WSQLite3Error):
    ...


class IdError(WSQLite3Error):
    ...


class FatalError(WSQLite3Error):
    ...


class Operator:
    server: Server
    current_connection: Connection
    session_connection: Connection
    response_connection: Connection
    order_grid: OrderedDict[str, Callable[..., Coroutine]]
    error_actions: dict[str | None, Callable[[bool], None]]
    section_order = (
        "ping",
        "_exec",
        "_getattr",
        "connection",
        "server",
        "thread",
        "sql",
        "broadcast",
    )

    class _CancelSignal(Exception):
        def __init__(self, sql_rollback: bool): self.sql_rollback = sql_rollback

    class CancelOrder(_CancelSignal):
        ...

    class CancelSession(_CancelSignal):
        ...

    class DestroyConnection(_CancelSignal):
        ...

    class ShutdownServer(_CancelSignal):
        def __init__(self, sql_rollback: bool, force: bool):
            Operator._CancelSignal.__init__(self, sql_rollback)
            self.force = force

    def erract_cancel_order(self, sql_rollback: bool):
        raise self.CancelOrder(sql_rollback)

    def erract_cancel_session(self, sql_rollback: bool):
        raise self.CancelSession(sql_rollback)

    def erract_destroy_connection(self, sql_rollback: bool):
        raise self.DestroyConnection(sql_rollback)

    def erract_shutdown_server(self, sql_rollback: bool):
        raise self.ShutdownServer(sql_rollback, False)

    def erract_force_shutdown_server(self, sql_rollback: bool):
        raise self.ShutdownServer(sql_rollback, True)

    def __init__(self, server: Server):
        self.server = server
        self.order_grid = OrderedDict((
            ("ping", self.proc_order__ping),
            ("_exec", self.proc_order___exec),
            ("_getattr", self.proc_order___getattr),
            ("connection", self.proc_order__connection),
            ("server", self.proc_order__server),
            ("thread", self.proc_order__thread),
            ("sql", self.proc_order__sql),
            ("broadcast", self.proc_order__broadcast),
        ))
        self.error_actions = {
            "cancel order": self.erract_cancel_order,
            "cancel session": self.erract_cancel_session,
            "ignore": lambda _: None,
            None: self.erract_cancel_session,
            "destroy connection": self.erract_destroy_connection,
            "shutdown server": self.erract_shutdown_server,
            "force shutdown server": self.erract_force_shutdown_server,
        }

    def exception_message_formatter(self, exc: Exception) -> Any:
        return {
            "type": type(exc).__name__,
            "args": exc.__dict__.get("args"),
            "repr": repr(exc),
            "message": str(exc),
            "pypickle": pickle.dumps(exc).__repr__(),
            "params": exc.__dict__.get("params"),
        }

    def exception_handle(self, order: dict, res: dict, exc: Exception):
        if isinstance(exc, FatalError):
            raise exc
        res["error"] = self.exception_message_formatter(exc)
        self.error_actions.get(
            (act := order.pop("error", "")).replace("+rollback", "").strip(), self.error_actions[None]
        )("+rollback" in act)

    def json_default(self, obj: object):
        if isinstance(obj, bytes):
            return b64encode(obj).decode()
        elif isinstance(obj, Sequence):
            return list(obj)
        elif isinstance(obj, Mapping):
            return dict(obj)
        elif isinstance(obj, Iterable):
            return list(obj)
        else:
            return repr(obj)

    def tb_params(self, _tb_params: list | dict) -> list | dict:
        for i in (_tb_params if isinstance(_tb_params, dict) else range(len(_tb_params))):
            val = _tb_params[i]
            if isinstance(val, str):
                if val.startswith("b:"):
                    _tb_params[i] = b64decode(val[2:])
                elif val.startswith("t:"):
                    _tb_params[i] = val[2:]
                else:
                    raise OrderError('each string in "tb:params" must be prefixed by "t:" or "p:"')
        return _tb_params

    def deserialize_input(self, payload: bytes) -> dict | list[dict]:
        """deserialize payload from stream"""
        return json.loads(payload)

    def serialize_output(self, obj: dict | object) -> bytes:
        """serialize object for stream"""
        return json.dumps(obj, default=self.json_default).encode()

    async def proc_order__ping(self, order: dict, res: dict):
        try:
            order.pop("ping", None)
            res["pong"] = time_ns()
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order___exec(self, order: dict, res: dict):
        try:
            exec(order.pop("code"), globals(), locals())
            res["result"] = globals().get("result") or locals().get("result")
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order___getattr(self, order: dict, res: dict):
        try:
            of = {
                "connection": self.current_connection,
                "server": self.server,
                "thread": self.current_connection.thread,
            }[order.pop("of")]
            path = order.pop("path").split(".")
            result = getattr(of, path.pop(0))
            while path:
                result = getattr(result, path.pop(0))
            call: tuple[tuple, dict] | None
            if call := order.pop("call", None):
                res["result"] = result(*call[0], **call[1])
            else:
                res["result"] = result
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order__connection(self, order: dict, res: dict):
        try:
            conn = self.current_connection
            if get := order.pop("get", ""):
                conn = self.server.get_connection(get)
                set_ = order.pop("set", None)
                if set_ == "session":
                    self.session_connection = conn
                elif set_ == "response":
                    self.response_connection = conn
                elif set_:
                    self.current_connection = conn
            if order.pop("id", None):
                res["id"] = conn.id
            if send := order.pop("send", None):
                await conn.send_broadcast(self.serialize_output(send))
            if desc := order.pop("description", None):
                if isinstance(desc, dict):
                    if pop := desc.pop("pop", ""):
                        res["description.pop"] = conn.description_pop(pop)
                    if _set := desc.pop("set", {}):
                        conn.description_set(_set)
                    if update := desc.pop("update", {}):
                        conn.description_update(update)
                res["description"] = conn.description
            if order.pop("properties", None):
                res["properties"] = conn.properties
            if order.pop("destroy", None):
                conn.destroy()
                raise Connection.CloseSignal
        except Connection.CloseSignal:
            raise
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order__server(self, order: dict, res: dict):
        try:
            if order.pop("connections", None):
                res["connections"] = {
                    conn.id: conn.properties
                    for conn in self.server.all_connections
                }
            if order.pop("threads", None):
                res["threads"] = {
                    threads.id: threads.properties
                    for threads in self.server.threads
                }
            if shutdown := order.pop("shutdown", None):
                self.server.shutdown(shutdown == "force")
                raise Connection.CloseSignal
        except Connection.CloseSignal:
            raise
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order__thread(self, order: dict, res: dict):
        try:
            thread = self.current_connection.thread
            if get := order.pop("get", ""):
                thread = self.server.get_thread(get)
            if order.pop("id", None):
                res["id"] = thread.id
            if broadcast := order.pop("broadcast", None):
                await thread.broadcast(self.serialize_output(
                    {
                        "broadcast": broadcast,
                        "from": thread.id,
                    }
                ), None if order.pop("broadcast.self", False) else self.current_connection)
            if order.pop("connections", None):
                res["connections"] = {
                    conn.id: conn.properties
                    for conn in thread.connections
                }
            if order.pop("properties", None):
                res["properties"] = thread.properties
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def proc_order__sql(self, order: dict, res: dict):
        sql: Callable[[], Database | None]
        def sql(): return None
        try:
            if order.pop("keys", None):
                res["keys"] = list(self.server.databases.keys())

            if open_ := order.pop("open", ()):
                _sql = self.server.add_database(order.pop("get", None), *open_[0], **open_[1])
                res["open"] = _sql.session_key

                def sql(): return _sql
            else:
                def sql():
                    nonlocal sql
                    _sql = self.server.get_database(order.pop("get", None))
                    def sql(): return _sql
                    return _sql

            side, force, sudo = order.pop("side", False), order.pop("force", False), order.pop("sudo", False)

            def exec_cur():
                return sql().cursor(self.current_connection, "sql", side, force, sudo)

            def fetch_cur():
                return sql().cursor(self.current_connection, "fetch", side, force, sudo)

            def release_cur():
                return sql().cursor(self.current_connection, "release", side, force, sudo)

            def close_cur():
                return sql().cursor(self.current_connection, "close", side, force, False)

            def attr_cur():
                return sql().cursor(self.current_connection, "_attr", side, False, False)

            exec_f: Callable | None = None

            def set_exec_f(_f: Callable):
                nonlocal exec_f
                if exec_f:
                    raise OrderError('only one sql execution per order is allowed ("exec", "script" or "many")')
                else:
                    exec_f = _f

            fetch_f: Callable | None = None

            def set_fetch_f(_f: Callable):
                nonlocal fetch_f
                if fetch_f:
                    raise OrderError('only one fetch operation per order is allowed ("fetchone", "fetchmany" or "fetchall")')
                else:
                    fetch_f = _f

            if e := order.pop("exec", ""):
                def __exec_f():
                    if params := order.pop("tb:params", []):
                        self.tb_params(params)
                    else:
                        params = order.pop("params", [])
                    with exec_cur() as cur:
                        cur.execute(e, params)

                set_exec_f(__exec_f)

            if s := order.pop("script", ""):
                if order.get("params") or order.get("tb:params"):
                    raise OrderError("sql.script does not support parameters (use sql.exec instead)")

                def __exec_f():
                    with exec_cur() as cur:
                        cur.executescript(s)

                set_exec_f(__exec_f)

            if m := order.pop("many", ""):
                def __exec_f():
                    if params := order.pop("tb:params", []):
                        params = (self.tb_params(p) for p in params)
                    else:
                        params = order.pop("params", [])
                    with exec_cur() as cur:
                        cur.executemany(m, params)

                set_exec_f(__exec_f)

            if order.pop("fetchone", None):
                def __fetch_f():
                    with fetch_cur() as cur:
                        return cur.fetchone()

                set_fetch_f(__fetch_f)

            if size := order.pop("fetchmany", None):
                if type(size) != int:
                    size = None

                def __fetch_f():
                    with fetch_cur() as cur:
                        return cur.fetchmany(size)

                set_fetch_f(__fetch_f)

            if fetchall := order.pop("fetchall", None):
                def __fetch_f():
                    with fetch_cur() as cur:
                        return cur.fetchall()

                set_fetch_f(__fetch_f)

            if order.pop("lock", None):
                with attr_cur() as cur:
                    res["lock"] = cur.lock_id

            if arraysize := order.pop("arraysize", None):
                with attr_cur() as cur:
                    if type(arraysize) == int:
                        cur.arraysize = arraysize
                    res["arraysize"] = cur.arraysize
            if order.pop("description", None):
                with attr_cur() as cur:
                    res["description"] = cur.description
            if order.pop("lastrowid", None):
                with attr_cur() as cur:
                    res["lastrowid"] = cur.lastrowid
            if order.pop("rowcount", None):
                with attr_cur() as cur:
                    res["rowcount"] = cur.rowcount

            if exec_f:
                exec_f()
            if fetch_f:
                res["fetch"] = fetch_f()

            if release := order.pop("release", None):
                if release == "finally":
                    if fetchall or (fetch_f and not res["fetch"]):
                        res["release"] = release_cur()
                else:
                    res["release"] = release_cur()

            if order.pop("rollback", None):
                with sql():
                    sql().connection.rollback()

            if order.pop("commit", None):
                with sql():
                    sql().connection.commit()

            if order.pop("close", None):
                res["close"] = close_cur()

        except Exception as exc:
            try:
                self.exception_handle(order, res, exc)
            except self._CancelSignal as e:
                if e.sql_rollback and sql():
                    sql().connection.rollback()
                raise
            return 2
        return 0

    async def proc_order__broadcast(self, order: dict, res: dict):
        try:
            msg = order.pop("message")
            await self.server.broadcast(self.serialize_output(
                {
                    "broadcast": msg,
                    "from": self.current_connection.id,
                }
            ), None if order.pop("self", False) else self.current_connection)
        except Exception as exc:
            self.exception_handle(order, res, exc)
            return 2
        return 0

    async def read_orders(self, order_root: dict, res_order: dict):
        err = 0
        for k, m in self.order_grid.items():
            try:
                if order := order_root.pop(k, None):
                    res = res_order.setdefault(k, dict())
                    err = (await m(order, res)) | err
            except Exception:
                res_order["error"] = res.get("error")
                raise
        return err

    async def __call__(self, conn: Connection, order_payload: bytes):
        self.session_connection = self.response_connection = conn
        try:
            order_chain: dict | list[dict] = self.deserialize_input(order_payload)

            if isinstance(order_chain, dict):
                order_chain = [order_chain]

            orders_out = list()
            res_order = dict()
            err = 0
            try:
                for order_part in order_chain:
                    self.current_connection = self.session_connection
                    orders_out.append(res_order := dict())
                    try:
                        err = (await self.read_orders(order_part, res_order)) or err
                    except self.CancelOrder:
                        err = 3
                    res_order["errors"] = err
            except self.CancelSession:
                err = 4
                res_order["errors"] = err

            await self.response_connection.send_response(
                self.serialize_output(
                    {
                        "orders": orders_out,
                        "errors": err,
                        "error": res_order.get("error")
                    }
                )
            )

        except Connection.CloseSignal:
            raise
        except self.DestroyConnection as e:
            _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, '<DestroyConnection> raised')
            self.current_connection.destroy()
        except self.ShutdownServer as e:
            _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, f'<ShutdownServer(force={e.force})> raised')
            self.server.shutdown(e.force)
        except Exception as e:
            _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, 'unexpected error raised while processing order -> sending { "error": {...} }')
            try:
                await self.response_connection.send_response(
                    self.serialize_output(
                        {
                            "errors": -1,
                            "error": self.exception_message_formatter(e)
                        }
                    )
                )
            except Exception as e:
                _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, "unexpected error raised while handling above exception -> sending null-byte")
                try:
                    await self.response_connection.send_response(b"\x00")
                except Exception as e:
                    _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, f"unexpected error raised while handling above exception -> destroy connection: {self.response_connection}")
                    try:
                        if not self.response_connection.writer.is_closing():
                            self.response_connection.destroy()
                        else:
                            self.response_connection.thread.connections.discard(self.response_connection)
                    except Exception as e:
                        _FATAL_ERROR_HANDLE(self.server, self.current_connection.id, e, f"unexpected error raised while handling above exception -> forceful server shutdown")
                        self.server.shutdown(force=True)


class Cursor(sqlite3.Cursor):
    handler: Connection | Database
    database: Database
    sql_lock: None | Connection
    _check_for_sql_: Callable[..., Cursor]
    _check_for_fetch_: Callable[..., Cursor]
    _destroy_: Callable[..., None]
    _release_: Callable[..., bool]
    _flush_: Callable[..., None]
    _t_lock: threading.Lock

    @overload
    def check_for_sql(self, from_: Connection, force: bool) -> None:
        ...

    @overload
    def check_for_sql(self, from_: Connection, force: bool, sudo: bool) -> None:
        ...

    def check_for_sql(self, *args, **kwargs) -> None:
        self._check_for_sql_(*args, **kwargs)

    @overload
    def check_for_fetch(self, from_: Connection, force: bool) -> None:
        ...

    @overload
    def check_for_fetch(self, from_: Connection, force: bool, sudo: bool) -> None:
        ...

    def check_for_fetch(self, *args, **kwargs) -> None:
        self._check_for_fetch_(*args, **kwargs)

    @overload
    def destroy(self, from_: Connection, force: bool, sudo: bool) -> None:
        ...

    @overload
    def destroy(self, from_: Connection, force: bool) -> None:
        ...

    def destroy(self, from_: Connection, force: bool, *sudo: bool) -> None:
        self._destroy_(from_, force, sudo)

    @overload
    def release(self, from_: Connection, sudo: bool) -> bool:
        ...

    @overload
    def release(self, ) -> bool:
        ...

    def release(self, *args, **kwargs) -> bool:
        self._t_lock_acquire()
        try:
            v = self._release_(*args, **kwargs)
        finally:
            self._t_lock_release()
        return v

    @property
    def lock_id(self) -> str | None:
        return self.sql_lock.id if self.sql_lock else None

    def set_handler(self, handler: Connection | Database, database: Database):
        self.handler = handler
        self.database = database
        if isinstance(handler, Connection):
            self._t_lock = threading.Lock()

            def check_for_sql(from_: Connection, force: bool, *_, **__) -> Cursor:
                if not force and self.sql_lock:
                    raise CursorLockedError(f"side cursor is locked")
                self.sql_lock = from_
                return self

            def check_for_fetch(from_: Connection, force: bool, *_, **__) -> Cursor:
                if not force and not self.sql_lock:
                    raise CursorNotLockedError(f"side cursor is not locked")
                self.sql_lock = from_
                return self

            def release(*_, **__) -> bool:
                _r = bool(self.sql_lock)
                self.sql_lock = None
                return _r

            def destroy(___, force: bool, *_, **__):
                def _destroy():
                    database.side_cursors.pop(handler)
                    handler._side_cursors.remove(self)
                    super(Cursor, self).close()

                if force:
                    _destroy()
                else:
                    with self:
                        _destroy()

        else:
            self._t_lock = handler._t_lock

            def check_for_sql(from_: Connection, force: bool, sudo: bool) -> Cursor:
                if self.sql_lock:
                    if self.sql_lock != from_:
                        if not sudo:
                            raise CursorLockedError(f"main cursor locked by", self.sql_lock.id)
                    elif not force:
                        raise CursorLockedError(f"main cursor locked by last order", self.sql_lock.id)
                self.sql_lock = from_
                return self

            def check_for_fetch(from_: Connection, force: bool, sudo: bool) -> Cursor:
                if self.sql_lock:
                    if self.sql_lock != from_ and not sudo:
                        raise CursorLockedError(f"main cursor locked by", self.sql_lock.id)
                elif not force:
                    raise CursorNotLockedError(f"main cursor is not locked")
                self.sql_lock = from_
                return self

            def release(from_: Connection, sudo: bool) -> bool:
                if (_r := bool(self.sql_lock)) and not sudo and self.sql_lock != from_:
                    raise CursorLockedError(f"main cursor locked by", self.sql_lock.id)
                self.sql_lock = None
                return _r

            def destroy(from_: Connection, force: bool, sudo: bool):
                if (_r := bool(self.sql_lock)) and not sudo and self.sql_lock != from_:
                    raise CursorLockedError(f"main cursor locked by", self.sql_lock.id)
                if not force:
                    super(Cursor, self).close()
                    database.close()
                else:
                    with self:
                        super(Cursor, self).close()
                        database.close(force=True)

        self._check_for_sql_ = check_for_sql
        self._check_for_fetch_ = check_for_fetch
        self._destroy_ = destroy
        self._release_ = release

    def __init__(self, connection: sqlite3.Connection):
        self.sql_lock = None
        sqlite3.Cursor.__init__(self, connection)

    def _t_lock_acquire(self) -> Cursor:
        if not self._t_lock.acquire(timeout=2):
            raise FatalError(f"(t_lock) SQL access for {self.handler}")
        return self

    def _t_lock_release(self):
        self._t_lock.release()

    def __enter__(self):
        self._t_lock_acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._t_lock_release()


class _CursorSuit:
    cursor: Cursor
    check: Callable[[Connection, bool, bool], None]
    from_: Connection
    force: bool
    sudo: bool

    def __init__(
            self,
            cursor: Cursor,
            check: Callable[[Connection, bool, bool], None],
            from_: Connection,
            force: bool,
            sudo: bool,
    ):
        self.cursor = cursor
        self.check = check
        self.from_ = from_
        self.force = force
        self.sudo = sudo

    def __enter__(self) -> Cursor:
        self.cursor._t_lock_acquire()
        try:
            self.check(self.from_, self.force, self.sudo)
            return self.cursor
        except Exception:
            self.cursor._t_lock_release()
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor._t_lock_release()


class Database:
    session_key: Hashable | None
    connection: sqlite3.Connection
    main_cursor: Cursor
    side_cursors: dict[Connection, Cursor]
    _t_lock: threading.Lock
    server: Server
    factory_Cursor: Callable[..., Cursor]

    def __init__(
            self,
            server: Server,
            session_key: Hashable | None,
            connection: sqlite3.Connection | tuple[tuple, dict],
            factory_Cursor: Callable[..., Cursor] = Cursor,
    ):
        self.server = server
        self._t_lock = threading.Lock()
        if isinstance(connection, sqlite3.Connection):
            self.connection = connection
        else:
            args, kwargs = _MAKE_SQLITE3_PARAMS(connection)
            self.connection = sqlite3.connect(*args, **kwargs)
        self.factory_Cursor = factory_Cursor
        self.main_cursor = self.connection.cursor(factory_Cursor)
        self.main_cursor.set_handler(self, self)
        self.session_key = None
        if session_key is not None:
            self.session_key = session_key or uuid4().__str__()
        self.side_cursors = dict()

    def close(self, force: bool = False):
        """remove the ``Database`` from handling and close the ``sqlite3.Connection``,
        do not wait for the lock to be released if `force` is ``True``"""

        def _close():
            for conn, cur in self.side_cursors.copy().items():
                cur.destroy(conn, force)
            self.connection.close()
            self.server.databases.pop(self.session_key)

        if not force:
            _close()
        else:
            with self:
                _close()

    def cursor(self, from_: Connection, for_: Literal["sql", "fetch", "release", "close", "_attr"], side: bool, force: bool, sudo: bool) -> _CursorSuit | bool:
        if side:
            if for_ == "close":
                try:
                    self.side_cursors[from_].destroy(from_, force)
                    return True
                except KeyError:
                    return False
            else:
                try:
                    cur = self.side_cursors[from_]
                except KeyError:
                    cur = self.connection.cursor(self.factory_Cursor)
                    cur.set_handler(from_, self)
                    self.side_cursors[from_] = cur
                    from_._side_cursors.add(cur)
        else:
            cur = self.main_cursor
        match for_:
            case "sql":
                return _CursorSuit(cur, cur.check_for_sql, from_, force, sudo)
            case "fetch":
                return _CursorSuit(cur, cur.check_for_fetch, from_, force, sudo)
            case "release":
                return cur.release(from_, sudo)
            case "close":
                cur.destroy(from_, force, sudo)
                return True
            case "_attr":
                return _CursorSuit(cur, lambda _, __, ___: None, from_, force, sudo)
            case _:
                raise OrderError(f"cant get cursor {for_=}")

    def __enter__(self) -> None:
        self._t_lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._t_lock.release()


class ConnectionWorker:
    id: str
    server: Server
    thread: ConnectionsThread | None
    properties: dict

    def __init__(
            self,
            server: Server,
            thread: ConnectionsThread | None,
    ):
        self.id = uuid4().__str__()
        self.server = server
        self.thread = thread

    def __hash__(self) -> int:
        return self.id.__hash__()

    def __eq__(self, other: ConnectionWorker) -> bool:
        return self.id.__eq__(other.id)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.id})>"


class Connection(ConnectionWorker):
    class CloseSignal(Exception):
        ...

    ws_stream_reader: wsdatautil.ProgressiveStreamReader
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    sock: socket.socket
    address: tuple[str, int]
    description: dict
    _side_cursors: set[Cursor]
    ws_handshake_timeout: float | None
    _keep_alive: bool
    _t_lock: threading.Lock
    
    def __init__(
            self,
            server: Server,
            thread: ConnectionsThread | None,
            sock: socket.socket,
            addr: tuple[str, int],
            ws_handshake_timeout: float | None = 2,
    ):
        """
        :param server: the Server
        :param thread: the ConnectionsThread
        :param sock: the client socket
        :param addr: the client address
        :param ws_handshake_timeout: set the timeout in seconds until the handshake header must be transmitted
        """
        ConnectionWorker.__init__(self, server, thread)
        self.ws_stream_reader = wsdatautil.ProgressiveStreamReader("auto")
        self.ws_handshake_timeout = ws_handshake_timeout
        self.sock = sock
        self.address = addr
        self._side_cursors = set()
        self.description = dict()
        self.properties = {
            "id": self.id,
            "description": self.description,
            "timestamp": float(time()),
            "address": self.address,
            "thread": self.thread.properties if self.thread else None,
        }
        self._keep_alive = True
        self._t_lock = threading.Lock()

    def __enter__(self):
        self._t_lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._t_lock.release()

    async def ws_wait_handshake(self) -> wsdatautil.HandshakeRequest:
        """Wait for a ws handshake header."""
        handshake_data = await asyncio.wait_for(self.reader.readuntil(b'\r\n\r\n'), self.ws_handshake_timeout)
        return wsdatautil.HandshakeRequest.from_streamdata(handshake_data)

    async def ws_send_handshake_response(self, request: wsdatautil.HandshakeRequest) -> None:
        """Create the ws handshake header response and send it to the client."""
        self.writer.write(request.make_response().to_streamdata())
        await self.writer.drain()

    async def ws_handshake(self) -> bool:
        """Wait for a ws handshake header and send the response back to the client.
        Returns whether the hand handshake was transmitted within the timeout.
        If ``False`` is returned, `destroy` is executed."""
        try:
            await self.ws_send_handshake_response(
                await self.ws_wait_handshake()
            )
        except TimeoutError:
            return False
        else:
            return True

    async def read_one_frame(self) -> wsdatautil.Frame:
        """Read one ws frame."""
        while True:
            var: int = 2
            while isinstance(var, int):
                var = self.ws_stream_reader.progressive_read(
                    await self.reader.readexactly(var)
                )
            var: wsdatautil.Frame
            match var.opcode:
                case wsdatautil.OPCODES.CLOSE:
                    await self.at_ws_close(var)
                case wsdatautil.OPCODES.PING:
                    await self.at_ws_ping(var)
                case _:
                    return var

    async def read_frames_until_fin(self) -> list[wsdatautil.Frame]:
        """Read ws frames until a fin flag is reached."""
        frames: list[wsdatautil.Frame] = list()
        while not frames or not frames[-1].fin:
            frames.append(await self.read_one_frame())
        return frames

    async def read_iteration(self) -> None:
        """Main loop iteration.
        Waits for incoming ws frames and sends the payload data to the Operator."""
        frames = await self.read_frames_until_fin()
        with self:
            await self.server.operator(self, bytes().join(f.payload for f in frames))

    async def run(self) -> None:
        """the main loop"""
        if not await self.ws_handshake():
            self.destroy()
        try:
            while self._keep_alive:
                try:
                    await self.read_iteration()
                except self.CloseSignal:
                    break
                except asyncio.IncompleteReadError as e:
                    await self.at_connection_broken(e)
                    break
                except Exception as e:
                    if not await self.at_unexpected_error(e):
                        break
        except Exception as e:
            _FATAL_ERROR_HANDLE(self.server, self.id, e, "exception raised by errorhandler -> destroy connection")
        finally:
            self.destroy()

    async def start(self) -> None:
        """Create the StreamReader and StreamWriter object and start the main loop (``run``)"""
        reader, writer = await asyncio.open_connection(sock=self.sock)
        self.reader = reader
        self.writer = writer
        await self.run()

    async def at_ws_close(self, frame: wsdatautil.Frame) -> None:
        """Executed with a received close-frame. By default, ``WsClose`` is raised, which closes the connection."""
        # code, msg = wsdatautil.get_close_code_and_message_from_frame(frame)
        raise self.CloseSignal

    async def at_ws_ping(self, frame: wsdatautil.Frame) -> None:
        """Executed with a received pong-frame. A pong-frame is transmitted back by default."""
        self.writer.write(wsdatautil.FrameFactory.PongFrame().to_streamdata())
        await self.writer.drain()

    async def at_connection_broken(self, exc: asyncio.IncompleteReadError) -> None:
        """Is executed after a connection is broken by the client. Does nothing by default."""
        pass

    async def at_unexpected_error(self, exc: Exception) -> bool:
        """Executed when unexpected errors occur.
        The return value specifies whether the main loop is to be continued
        (is ``False`` by default)"""
        _FATAL_ERROR_HANDLE(self.server, self.id, exc, "unexpected error raised -> destroy connection")
        return False

    async def send_response(self, payload: bytes) -> None:
        """Generate ws frames with `payload` and `mask` and send them to the client.
        This method is used by the Operator to send the response."""
        self.writer.write(wsdatautil.Frame(
            payload,
            wsdatautil.OPCODES.BINARY,
        ).to_streamdata())
        await self.writer.drain()

    async def send_broadcast(self, payload: bytes) -> None:
        """Generate ws frames with `payload` and send them to the client."""
        self.writer.write(wsdatautil.Frame(
            payload,
            wsdatautil.OPCODES.BINARY,
        ).to_streamdata())
        await self.writer.drain()

    async def feed(self, payload: bytes) -> None:
        """Generate ws frames with `payload` and feed the reader."""
        self.reader.feed_data(wsdatautil.Frame(
            payload,
            wsdatautil.OPCODES.BINARY,
        ).to_streamdata())

    def description_set(self, desc: dict) -> None:
        """self.description = desc"""
        self.description.clear()
        self.description.update(desc)

    def description_update(self, desc: dict) -> None:
        """self.description |= desc"""
        self.description.update(desc)

    def description_pop(self, key: str) -> Any:
        """return self.description.pop(key)"""
        return self.description.pop(key)

    def destroy(self) -> None:
        """close the connection and remove the Connection object from the parent thread"""
        self._keep_alive = False
        self.reader.feed_eof()
        self.sock.close()
        self.writer.close()
        self.thread.connections.discard(self)
        for cur in self._side_cursors.copy():
            cur.destroy(self, False)


class ConnectionsThread(threading.Thread, ConnectionWorker):
    connections: set[Connection]
    async_loop: asyncio.AbstractEventLoop

    def __repr__(self) -> str:
        return ConnectionWorker.__repr__(self)

    def __init__(self, server: Server):
        ConnectionWorker.__init__(self, server, None)
        threading.Thread.__init__(
            self,
            # daemon=True,
            # ->
            # Fatal Python error: _enter_buffered_busy: could not acquire lock
            # for <_io.BufferedWriter name='<stdout>'> at interpreter shutdown,
            # possibly due to daemon threads
        )
        self.connections = set()

    def run(self) -> None:
        """run the async loop in this thread"""
        self.properties = {
            "id": self.id,
            "timestamp": float(time()),
            "thread.ident": self.ident,
            "TID": self.native_id,
        }
        self.async_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.async_loop)
        self.async_loop.run_forever()

    async def add_conn(self, sock: socket.socket, addr: tuple[str, int]) -> None:
        """Add a connection to this thread."""
        self.connections.add(
            conn := self.server.factory_Connection(
                self.server,
                self,
                sock,
                addr,
            )
        )
        asyncio.run_coroutine_threadsafe(conn.start(), self.async_loop)

    async def broadcast(self, payload: bytes, except_: Connection) -> None:
        """Send `payload` to all connections of this thread [`except_` <connection.id>]."""
        if except_:
            for conn in self.connections:
                if conn != except_:
                    asyncio.run_coroutine_threadsafe(conn.send_broadcast(payload), self.async_loop)
        else:
            for conn in self.connections:
                asyncio.run_coroutine_threadsafe(conn.send_broadcast(payload), self.async_loop)

    def destroy(self) -> None:
        """close all connections of the thread and stop the async loop"""
        for task in asyncio.all_tasks(self.async_loop):
            task.cancel()
        for conn in self.connections.copy():
            conn.destroy()
        filterwarnings(
            'ignore',
            message=r'^coroutine .* was never awaited$',
            category=RuntimeWarning
        )
        self.async_loop.call_soon_threadsafe(self.async_loop.stop, )  # type-hint-bug: Unpack[_Ts]


class Server(threading.Thread):
    databases: dict[Any, Database]
    address: tuple[str, int]
    sock: sock.socket
    threads: set[ConnectionsThread]
    _conn_req_: Callable[[socket.socket, tuple[str, int]], None | Coroutine]
    operator: Operator
    async_loop: asyncio.AbstractEventLoop
    _keep_alive: bool = True
    factory_Connection: Callable[..., Connection] | Type[Connection]
    factory_Database: Callable[..., Database] | Type[Database]

    def __init__(
            self,
            host: str,
            port: int,
            threads: int = 1,
            connections_per_thread: int = 0,
            factory_Connection: Callable[..., Connection] = Connection,
            factory_ConnectionsThread: Callable[..., ConnectionsThread] = ConnectionsThread,
            factory_Operator: Callable[..., Operator] = Operator,
            factory_Database: Callable[..., Database] = Database,
    ):
        """
        :param host: server host
        :param port: server port
        :param threads: count of sub threads
        :param connections_per_thread: Limit the number of connections per thread. Numbers less than 1 correspond to no limit (default)
        :param factory_Connection: receives the parameters and must return a Connection instance
        :param factory_ConnectionsThread: receives the parameters and must return an ConnectionsThread instance
        :param factory_Operator: receives the parameters and must return an Operator instance
        :param factory_Database: receives the parameters and must return a Database instance
        """
        threading.Thread.__init__(self)
        self.databases = dict()
        self.threads = set()
        self.address = (host, port)
        self.factory_Connection = factory_Connection
        self.factory_Database = factory_Database

        for _ in range(max(1, threads)):
            ct = factory_ConnectionsThread(self)
            ct.start()
            self.threads.add(ct)

        if connections_per_thread > 0:
            async def _conn_req_(sock: socket.socket, addr: tuple[str, int]):
                for ct in self.threads:
                    if len(ct.connections) < connections_per_thread:
                        await ct.add_conn(sock, addr)
                        break
                else:
                    sock.close()
        else:
            async def _conn_req_(sock: socket.socket, addr: tuple[str, int]):
                await min(self.threads, key=lambda c: len(c.connections)).add_conn(sock, addr)

        self._conn_req_ = _conn_req_

        self.operator = factory_Operator(self)

    @overload
    def add_database(self, session_key: Hashable | None, sqlite3_connect_database: str | bytes | PathLike[str] | PathLike[bytes], /, **sqlite3_connect_kwargs) -> Database:
        ...

    @overload
    def add_database(self, session_key: Hashable | None, sql_connection: sqlite3.Connection, /) -> Database:
        ...

    @overload
    def add_database(self, session_key: Hashable | None, *sqlite3_connect_args, **sqlite3_connect_kwargs) -> Database:
        ...

    def add_database(self, session_key: Hashable | None, *sqlite3_connect_args, **sqlite3_connect_kwargs) -> Database:
        """Add a database connection for handling. `session_key` defines the key under which the connection is accessible,
        this must be ``truthy`` or ``None`` for a default database that does not need to be explicitly requested by the connection.
        If the next parameter is not an existing ``sqlite3.Connection``, the remaining parameters
        are passed to ``sqlite3.connect`` and a database is opened.
        
        ``sqlite3.connect(...,``\\ ``check_same_thread=False)``
            Note that an existing ``sqlite3.Connection`` must be designed for multithreading.

        ``_MAKE_SQLITE3_PARAMS``
            Since the signature of ``sqlite3.connect`` can change with the python versions (especially 3.12 and 3.15),
            the parameterization is not defined in detail and positional parameters are converted into keyword parameters
            before they are passed to ``sqlite3.connect``. In addition, the parameter `check_same_thread` is automatically
            set to ``False``.
        """
        if session_key is None and (sql := self.databases.get(session_key)):
            raise ConfigurationError(f"default database set: {sql}")
        elif session_key and (sql := self.databases.get(session_key)):
            raise ConfigurationError(f"{session_key!r}: {sql}")
        if sqlite3_connect_args and isinstance(sqlite3_connect_args[0], sqlite3.Connection):
            sql = self.factory_Database(self, session_key, sqlite3_connect_args[0])
        else:
            sql = self.factory_Database(self, session_key, (sqlite3_connect_args, sqlite3_connect_kwargs))
        self.databases[sql.session_key] = sql
        return sql

    def close_database(self, session_key: Hashable | None, force: bool = False) -> None:
        """remove a ``Database`` from handling and close the ``sqlite3.Connection``,
        do not wait for the lock to be released if `force` is ``True``"""
        if not (sql := self.databases.pop(session_key, None)):
            raise ConfigurationError(f"{session_key!r} unset")
        sql.close(force)

    def get_database(self, session_key: Hashable | None) -> Database:
        try:
            return self.databases[session_key]
        except KeyError:
            if session_key:
                raise ConfigurationError(f"{session_key!r} unset")
            else:
                raise ConfigurationError(f"default database unset")

    @property
    def all_connections(self) -> set[Connection]:
        """set of all connections"""
        conns = set()
        for ct in self.threads:
            conns |= ct.connections
        return conns

    def get_connection(self, by: str | dict) -> Connection:
        """get connection `by` id (str) or description records (dict)"""
        if isinstance(by, dict):
            def comp():
                for k, v in by.items():
                    try:
                        if conn.description[k] != v:
                            return False
                    except KeyError:
                        return False
                else:
                    return True
        else:
            def comp():
                return conn.id == by

        for conn in self.all_connections:
            if comp():
                return conn
        else:
            raise IdError(by)

    def get_thread(self, id: str) -> Connection | ConnectionsThread:
        """get thread by `id`"""
        for thread in self.threads:
            if thread.id == id:
                return thread
        else:
            raise IdError(id)

    async def connection_request(self, sock: socket.socket, addr: tuple[str, int]) -> None:
        """process a connection request"""
        await self._conn_req_(sock, addr)

    async def open_socket(self):
        """open the socket"""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(self.address)
        self.sock.listen()

    async def serve(self) -> None:
        """open the socket and run the mainloop"""
        await self.open_socket()
        self.async_loop = asyncio.get_event_loop()
        with self.sock:
            while self._keep_alive:
                await self.connection_request(*self.sock.accept())

    def run(self) -> None:
        """asyncio.run(self.serve())"""
        return asyncio.run(self.serve())

    def start(self, wait_iterations: int | bool = False, wait_time: float = .001):
        super().start()
        if wait_iterations:
            for i in range((1000 if isinstance(wait_iterations, bool) else wait_iterations)):
                try:
                    if self.async_loop.is_running():
                        return
                except AttributeError:
                    pass
                sleep(wait_time)
            else:
                raise TimeoutError

    async def broadcast(self, payload: bytes, except_: Connection) -> None:
        """Send `payload` to all connections [`except_` <connection.id>]."""
        if except_:
            for conn in self.all_connections:
                if conn != except_:
                    asyncio.run_coroutine_threadsafe(conn.send_broadcast(payload), conn.thread.async_loop)
        else:
            for conn in self.all_connections:
                asyncio.run_coroutine_threadsafe(conn.send_broadcast(payload), conn.thread.async_loop)

    def shutdown(self, force: bool = False) -> None:
        """Close all connections and shut down the server.
        Do not wait for lock's if `force` is ``True``."""

        for sql in self.databases.copy().values():
            try:
                sql.close(force)
            except Exception as e:
                _FATAL_ERROR_HANDLE(self, "", e, f"shutdown @ close {sql}")

        self._keep_alive = False

        async def _conn_req_(*args):
            pass

        self._conn_req_ = _conn_req_
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(.1)
                sock.connect(self.address)
        except Exception as e:
            _FATAL_ERROR_HANDLE(self, "", e, "shutdown @ connecting to own socket")
        for task in asyncio.all_tasks():
            task.cancel()
        try:
            self.sock.close()
        except Exception as e:
            _FATAL_ERROR_HANDLE(self, "", e, "shutdown @ close socket")
        for thread in self.threads.copy():
            try:
                thread.destroy()
            except Exception as e:
                _FATAL_ERROR_HANDLE(self, "", e, f"shutdown @ destroy thread {thread.id}")
