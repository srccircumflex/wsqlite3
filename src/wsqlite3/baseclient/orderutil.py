from __future__ import annotations

import pickle
from typing import Any, Literal, Sequence, Mapping, Type, TypeVar, Hashable, overload

try:
    from .baseclient import Connection
except ImportError:
    pass


_T = TypeVar("_T")


class _OrderSection:
    __data__: dict
    __root__: _OrderSection

    def _init(self, key: str, parent: _OrderSection | None):
        parent[key] = self
        self.__communicate__ = parent.__communicate__

    def __init__(self, key: str, parent: _OrderSection):
        self.__data__ = dict()
        self._init(key, parent)
        self.__root__ = (parent.__root__ if parent is not None else self)

    def _instruction(self, key: str, val: Any):
        self[key] = val
        return self

    def _section(self, key: str, val: Type[_T], *args) -> _T:
        try:
            return self[key]
        except KeyError:
            val = val(self, *args)
            self[key] = val
            return val

    def error_action(
            self,
            action: Literal[
                "cancel order",
                "cancel session",
                "ignore",
                "destroy connection",
                "shutdown server",
                "force shutdown server",
                "... +rollback"
            ] | str
    ):
        self._instruction("error", action)

    def __getitem__(self, item):
        return self.__data__.__getitem__(item)

    def __setitem__(self, key, value):
        return self.__data__.__setitem__(key, value)

    def _setdefault(self, key, default):
        return self.__data__.setdefault(key, default)

    def _pop(self, key, default):
        return self.__data__.pop(key, default)

    def __communicate__(self, block: bool = True, timeout: int | None = None) -> list[dict]:
        ...

    def __repr__(self):
        return repr(self.__data__)


class _OSecPing(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("ping", parent)
        self._instruction("ping", 1)


class _OSecExec(_OrderSection):

    def __init__(self, parent: _OrderSection, code: str):
        super().__init__("_exec", parent)
        self._instruction("code", code)


class _OSecGetattr(_OrderSection):

    def __init__(
            self,
            parent: _OrderSection,
            of: Literal[
                "connection",
                "server",
                "thread",
            ],
            path: str,
            call: Sequence[Sequence, Mapping] | None = None
    ):
        super().__init__("_getattr", parent)
        self._instruction("of", of)
        self._instruction("path", path)
        if call:
            self._instruction("call", call)


class _OSecConnection(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("connection", parent)

    def get(self, id_or_description_records: str | dict, set_for: Literal["section", "order", "session", "response"] | None = "section"):
        return self._instruction("get", id_or_description_records)._instruction("set", (None if set_for == "section" else set_for))

    def id(self):
        return self._instruction("id", 1)

    def send(self, serializable: object):
        return self._instruction("send", serializable)

    def description(self):
        return self._instruction("description", 1)

    def description_pop(self, key: Any):
        return self._instruction("description.pop", key)

    def description_set(self, description: dict):
        return self._instruction("description.set", description)

    def description_update(self, description: dict):
        return self._instruction("description.update", description)

    def properties(self):
        return self._instruction("properties", 1)

    def destroy(self):
        return self._instruction("destroy", 1)


class _OSecServer(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("server", parent)

    def connections(self):
        return self._instruction("connections", 1)

    def threads(self):
        return self._instruction("threads", 1)

    def shutdown(self, force: bool = False, commit: bool = False):
        if force:
            if commit:
                val = "force,commit"
            else:
                val = "force"
        elif commit:
            val = "commit"
        else:
            val = 1
        return self._instruction("shutdown", val)


class _OSecThread(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("thread", parent)

    def get(self, id: str):
        return self._instruction("get", id)

    def id(self):
        return self._instruction("id", 1)

    def broadcast(self, serializable: object, to_self: bool = False):
        self._instruction("broadcast", serializable)
        return self._instruction("broadcast.self", to_self)

    def connections(self):
        return self._instruction("connections", 1)

    def properties(self):
        return self._instruction("properties", 1)


class _OSecWs(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("ws", parent)

    def i_masking(self, i_masking: bool | Literal["auto"]):
        return self._instruction("i_masking", i_masking)

    def o_masking(self, o_masking: str | bool | dict[Literal["if", "else"], bool | Literal["random", "same"] | str] | dict):
        return self._instruction("o_masking", o_masking)

    def payload_size(self, size: int | None):
        return self._instruction("payload_size", size)

    def broadcast_masking(self, mask: bool | Literal["random"] | str):
        return self._instruction("broadcast_masking", mask)


class _OSecSql(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("sql", parent)

    def keys(self):
        return self._instruction("keys", 1)

    def open(self, sqlite3_connect_args: tuple = None, sqlite3_connect_kwargs: dict = None):
        return self._instruction("open", (sqlite3_connect_args or (), sqlite3_connect_kwargs or {}))

    def get(self, session_id: Hashable | None):
        return self._instruction("get", session_id)

    def side(self):
        return self._instruction("side", 1)

    def force(self):
        return self._instruction("force", 1)

    def sudo(self):
        return self._instruction("sudo", 1)

    def lock(self):
        return self._instruction("lock", 1)

    def arraysize(self, set_size: int | None = None):
        return self._instruction("arraysize", set_size is None or set_size)

    def description(self):
        return self._instruction("description", 1)

    def lastrowid(self):
        return self._instruction("lastrowid", 1)

    def rowcount(self):
        return self._instruction("rowcount", 1)

    def script(self, sql: str):
        self._pop("exec", None)
        self._pop("many", None)
        return self._instruction("script", sql)

    @overload
    def exec(self, sql: str, *, params: Sequence | Mapping = ()):
        return self

    @overload
    def exec(self, sql: str, *, tb_params: Sequence | Mapping = ()):
        return self

    def exec(self, sql: str, **__params: Sequence | Mapping):
        self._pop("script", None)
        self._pop("many", None)
        if params := __params.get("tb_params"):
            self._instruction("tb:params", params)
        elif params := __params.get("params"):
            self._instruction("params", params)
        return self._instruction("exec", sql)

    @overload
    def many(self, sql: str, *, params: Sequence | Mapping):
        return self

    @overload
    def many(self, sql: str, *, tb_params: Sequence | Mapping):
        return self

    def many(self, sql: str, **__params: Sequence[Sequence | Mapping]):
        self._pop("exec", None)
        self._pop("script", None)
        if params := __params.get("tb_params"):
            self._instruction("tb:params", params)
        elif params := __params.get("params"):
            self._instruction("params", params)
        return self._instruction("many", sql)

    def fetchone(self):
        return self._instruction("fetchone", 1)

    def fetchall(self):
        return self._instruction("fetchall", 1)

    def fetchmany(self, size: int | None = None):
        return self._instruction("fetchone", size is None or size)

    def release(self, finally_: bool = False):
        return self._instruction("release", ("finally" if finally_ else 1))

    def rollback(self):
        return self._instruction("rollback", 1)

    def commit(self):
        return self._instruction("commit", 1)

    def close(self):
        return self._instruction("close", 1)


class _OSecBroadcast(_OrderSection):

    def __init__(self, parent: _OrderSection, serializable: object, to_self: bool = False):
        super().__init__("broadcast", parent)
        self._instruction("message", serializable)._instruction("self", to_self)


class _OSecAutoclose(_OrderSection):

    def __init__(self, parent: _OrderSection):
        super().__init__("autoclose", parent)

    def cancel(self, mode: Literal["cancel", "explicit not", "force"] = "cancel"):
        return self._instruction("cancel", {"cancel": 1, "explicit not": -1, "force": 2}[mode])

    def value(self, val: Literal["block", "request"]):
        return self._instruction("value", val)

    def config(self):
        return self._instruction("config", 1)

    def config_block(self, enable: bool):
        return self._instruction("config.block", enable)

    def config_request(self, enable: bool):
        return self._instruction("config.request", enable)

    def config_wait_response(self, val: float):
        return self._instruction("config.wait_response", val)

    def config_wait_close(self, val: float):
        return self._instruction("config.wait_close", val)

    def config_force_shutdown(self, val: bool = True):
        return self._instruction("config.force_shutdown", val)

    def config_sql_commit(self, val: bool = True):
        return self._instruction("config.sql_commit", val)

    def trigger(self):
        return self._instruction("trigger", 1)
    

class _Error(Exception):
    type: str | None
    args: tuple | None
    repr: str | None
    message: str | None
    params: Any | None
    pypickle: str | None
    action_code: int

    def __init__(self, action_code: int, error: dict):
        self.type = error.get("type")
        self.args = error.get("args") or ()
        self.repr = error.get("repr")
        self.message = error.get("message")
        self.params = error.get("params")
        self.pypickle = error.get("pypickle")
        self.action_code = action_code

    def raise_origin(self):
        if self.pypickle:
            raise pickle.loads(eval(self.pypickle)) from self
        else:
            raise self


class ClientError(_Error):
    ...


class FatalError(_Error):
    ...


class Order(_OrderSection):

    client: Connection
    _block: bool
    _timeout: int | None

    def _init(self, key: str, parent: _OrderSection): pass

    def __init__(self, client: Connection, block: bool = True, timeout: int | None = None, flag=None):
        _OrderSection.__init__(self, None, None)
        self.client = client
        self._block = block
        self._timeout = timeout
        self["flag"] = flag

    def ping(self):
        return self._section("ping", _OSecPing)

    def exec(self, code: str):
        return self._section("_exec", _OSecExec, code)

    def getattr(
            self,
            of: Literal[
                "connection",
                "server",
                "thread",
            ],
            path: str,
            call: Sequence[Sequence, Mapping] | None = None
    ):
        return self._section("_getattr", _OSecGetattr, of, path, call)

    def connection(self):
        return self._section("connection", _OSecConnection)

    def server(self):
        return self._section("server", _OSecServer)

    def thread(self):
        return self._section("thread", _OSecThread)

    def ws(self):
        return self._section("ws", _OSecWs)

    def sql(self):
        return self._section("sql", _OSecSql)

    def broadcast(self, serializable: object, to_self: bool = False):
        return self._section("broadcast", _OSecBroadcast, serializable, to_self)

    def autoclose(self):
        return self._section("autoclose", _OSecAutoclose)

    def error_action(self, _):
        raise ValueError('"error" handling not supported in order root')

    def __communicate__(self, block: bool = -1, timeout: int | None = -1) -> list[dict]:
        if block == -1:
            block = self._block
        if timeout == -1:
            timeout = self._timeout
        return self.client.communicate(self, block, timeout)

    def __enter__(self) -> Order:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__communicate__()
