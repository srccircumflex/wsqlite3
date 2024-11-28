from __future__ import annotations

import base64
import sys
import pathlib

try:
    sys.path.insert(0, str(pathlib.Path(__file__).parent))
except Exception:
    raise

import asyncio
import json
import queue
import socket
import sqlite3
import threading
from collections import OrderedDict
from collections.abc import Sequence, Mapping, Iterable
from http.client import responses
from random import randbytes
from traceback import format_exception
from typing import Callable, Coroutine, Any, Generator, Literal, IO, Hashable
from uuid import uuid4
import time
import multiprocessing

import unittest

import wsdatautil
from src import wsqlite3


def log_ok(msg: object = ""):
    print("", end="", flush=True)
    print("\x1b[32mOK:", msg, "\x1b[m", flush=True)


class TestConnectionLimit(unittest.TestCase):

    def test_single(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, connections_per_thread=1)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()
        resp = client.order().ping().__communicate__()

        client2 = wsqlite3.baseclient.Connection("localhost", 9092)
        with self.assertRaises(ConnectionResetError) as ar:
            client2.connect()
        client2.sock.close()

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_two_threads(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, threads=2, connections_per_thread=1)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()
        resp = client.order().ping().__communicate__()

        client2 = wsqlite3.baseclient.Connection("localhost", 9092)
        client2.start()
        resp2 = client.order().ping().__communicate__()

        client3 = wsqlite3.baseclient.Connection("localhost", 9092)
        with self.assertRaises(ConnectionResetError) as ar:
            client3.connect()
        client3.sock.close()

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_no_limit(self):
        for t in (4, 1):
            port = 9090 + t
            server = wsqlite3.verbose_service.DebugServer("localhost", port, threads=t, connections_per_thread=0)
            server.start(wait_iterations=True)

            clients = list()
            for i in range(20):
                client = wsqlite3.baseclient.Connection("localhost", port)
                client.connect()
                clients.append(client)

            for client in clients:
                client.sock.close()

            client = wsqlite3.baseclient.Connection("localhost", port)
            client.connect()
            with self.assertRaises(queue.Empty) as ar:
                client.order().server().shutdown().__communicate__(False)


class TestOrders(unittest.TestCase):

    def test_ping(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()

        resp = client.order().ping().__communicate__()[0]

        self.assertTrue(isinstance(resp["ping"]["pong"], int))

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test__exec(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()

        resp = client.order().exec("result = print(self.current_connection.thread.id * 100, '\\n', '#' * 100) or 42").__communicate__()[0]

        self.assertEqual(resp["_exec"]["result"], 42)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test__getattr(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()

        resp = client.order().getattr("server", "sock.getsockopt", ((socket.SOL_SOCKET, socket.SO_REUSEADDR), {})).__communicate__()[0]

        self.assertEqual(resp["_getattr"]["result"], 1)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_connection(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, threads=2)
        server.start(wait_iterations=True)

        client1 = wsqlite3.baseclient.Connection("localhost", 9092)
        client1.start()

        resp = client1.order().connection().id().__communicate__()[0]
        c1_id = resp["connection"]["id"]
        log_ok(resp)

        desc1 = {"c1": 1, "x": 0}
        resp = client1.order().connection().description_set(desc1).__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        desc1.pop("x")
        resp = client1.order().connection().description_pop("x").__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)
        self.assertEqual(resp["connection"]["description.pop"], 0)

        desc_upd = {"y": 1}
        desc1 |= desc_upd
        resp = client1.order().connection().description_update(desc_upd).__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        resp = client1.order().thread().properties().__communicate__()[0]
        log_ok(resp)
        t1_prop = resp["thread"]["properties"]

        resp = client1.order().connection().properties().__communicate__()[0]
        self.assertEqual(resp["connection"]["properties"]["description"], desc1)
        self.assertEqual(resp["connection"]["properties"]["thread"], t1_prop)
        self.assertEqual(resp["connection"]["properties"]["id"], c1_id)
        self.assertEqual(resp["connection"]["properties"]["address"], list(client1.sock.getsockname()))

        client2 = wsqlite3.baseclient.Connection("localhost", 9092)
        client2.start()

        desc2 = {"c2": 2, "x": 0}
        resp = client2.order().connection().description_set(desc2).__communicate__()[0]

        resp = client2.order().connection().get(c1_id).description_get().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        resp = client2.order().connection().description_get().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc2)

        resp = client2.order().connection().properties().__communicate__()[0]
        self.assertNotEqual(resp["connection"]["properties"]["thread"], t1_prop)

        resp = client2.order().connection().get({"y": 1}).id().__communicate__()[0]
        self.assertEqual(resp["connection"]["id"], c1_id)

        resp = client2.order().connection().get(c1_id).send({"": 42}).__communicate__()[0]
        self.assertEqual(client1.recv_obj(), {"": 42})

        resp = client2.order().connection().id().__communicate__()[0]
        c2_id = resp["connection"]["id"]
        log_ok(resp)

        with self.assertRaises(queue.Empty) as ar:
            client2.communicate([
                client2.order().connection().get(c1_id, "response"),
                client2.order().connection().id(),
            ], False)

        resp = client1.recv_obj()
        log_ok(resp)

        self.assertEqual(resp["orders"][1]["connection"]["id"], c2_id)

        with self.assertRaises(queue.Empty) as ar:
            client2.order().connection().destroy().__communicate__(False)

        with self.assertRaises(queue.Empty) as ar:
            client1.order().server().shutdown().__communicate__(False)

    def test_server(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()

        resp = client.order().server().connections().__communicate__()[0]
        log_ok(resp)

        resp = client.order().server().threads().__communicate__()[0]
        log_ok(resp)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_thread(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, threads=2)
        server.start(wait_iterations=True)

        broadcast_comp: Callable[[dict], ...]

        class ClientConn(wsqlite3.baseclient.Connection):

            async def at_broadcast(self, broadcast: dict) -> None:
                broadcast_comp(broadcast)
                return await super().at_broadcast(broadcast)

        client1 = ClientConn("localhost", 9092)
        client1.start()

        client2 = ClientConn("localhost", 9092)
        client2.start()

        c1_id = client1.order().connection().id().__communicate__()[0]["connection"]["id"]
        c2_id = client2.order().connection().id().__communicate__()[0]["connection"]["id"]

        t1_id = client1.order().connection().properties().__communicate__()[0]["connection"]["properties"]["thread"]["id"]

        resp = client1.order().server().threads().__communicate__()[0]
        resp["server"]["threads"].pop(t1_id)

        t2_id = list(resp["server"]["threads"])[0]

        resp = client1.order().thread().get(t2_id).connections().__communicate__()[0]
        self.assertIn(c2_id, resp["thread"]["connections"])

        resp = client1.order().thread().get(t2_id).properties().__communicate__()[0]
        self.assertEqual(resp["thread"]["properties"]["id"], t2_id)

        def _broadcast_comp(bc):
            log_ok(bc)
            self.assertEqual(bc, {"broadcast": 42, "from": t2_id})

        broadcast_comp = _broadcast_comp

        resp = client1.order().thread().get(t2_id).broadcast(42).__communicate__()[0]

        count = 0

        def _broadcast_comp(bc):
            nonlocal count
            count += 1
            log_ok(bc)
            self.assertEqual(bc["broadcast"], 42)

        broadcast_comp = _broadcast_comp

        resp = client1.order().thread().get(t2_id).broadcast(42, to_self=True).__communicate__()[0]

        time.sleep(1)
        self.assertEqual(count, 2)

        with self.assertRaises(queue.Empty) as ar:
            client1.order().server().shutdown().__communicate__(False)

    def test_sql(self):
        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, threads=2)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", 9092)
        client.start()

        try:
            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().exec(";").__communicate__()
            with self.assertRaises(wsqlite3.ConfigurationError):
                ar.exception.raise_origin()

            server.add_database(1, ":memory:")

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().exec(";").__communicate__()
            with self.assertRaises(wsqlite3.ConfigurationError):
                ar.exception.raise_origin()

            client.order().sql().get(2).open((":memory:",)).__communicate__()

            self.assertEqual(
                list(sorted(client.order().sql().keys().__communicate__()[0]["sql"]["keys"])),
                [1, 2]
            )

            client.order().sql().get(2).close().__communicate__()
            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().get(2).close().__communicate__()
            with self.assertRaises(wsqlite3.ConfigurationError):
                ar.exception.raise_origin()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().open((":memory:",)).exec("CREATE TABLE main (x INT, y INT \\\\\\\\\\").__communicate__()
            with self.assertRaises(sqlite3.OperationalError):
                ar.exception.raise_origin()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().open((":memory:",)).exec("CREATE TABLE main (x INT, y INT)").__communicate__()
            with self.assertRaises(wsqlite3.ConfigurationError):
                ar.exception.raise_origin()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().exec("CREATE TABLE main (x INT, y INT)").__communicate__()
            with self.assertRaises(wsqlite3.CursorLockedError):
                ar.exception.raise_origin()

            client.communicate([
                client.order().sql().release(),
                client.order().sql().exec("CREATE TABLE main (x INT, y INT)").release(),
            ])
            client.order().sql().exec("INSERT INTO main VALUES (11, ?)", params=(22,)).release().__communicate__()
            client.order().sql().exec("INSERT INTO main VALUES (99, :x)", params={"x": 00}).release().__communicate__()
            client.order().sql().exec("SELECT * FROM main").release().__communicate__()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                resp = client.order().sql().fetchone().__communicate__()
            with self.assertRaises(wsqlite3.CursorNotLockedError):
                ar.exception.raise_origin()

            resp = client.order().sql().force().fetchone().release().__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [11, 22])

            client2 = wsqlite3.baseclient.Connection("localhost", 9092)
            client2.start()

            client2.order().sql().exec("SELECT * FROM main").__communicate__()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                resp = client.order().sql().fetchone().__communicate__()
            with self.assertRaises(wsqlite3.CursorLockedError):
                ar.exception.raise_origin()

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                resp = client.order().sql().force().fetchone().__communicate__()
            with self.assertRaises(wsqlite3.CursorLockedError):
                ar.exception.raise_origin()

            resp = client.order().sql().sudo().fetchone().__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [11, 22])

            resp = client.order().sql().fetchone().__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [99, 00])

            resp = client.order().sql().fetchone().release(finally_=True).__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], None)

            resp = client2.order().sql().side().many("INSERT INTO main VALUES (5, ?)", params=((1,), (2,), (3,))).release().__communicate__()
            client2.order().sql().side().exec("SELECT * FROM main").__communicate__()
            resp = client.order().sql().force().sudo().fetchall().release(finally_=True).__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [])

            resp = client2.order().sql().side().fetchall().release(finally_=True).__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [[11, 22], [99, 00], [5, 1], [5, 2], [5, 3]])

            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                resp = client.order().sql().sudo().fetchall().release(finally_=True).__communicate__()
            with self.assertRaises(wsqlite3.CursorNotLockedError):
                ar.exception.raise_origin()

            client2.order().sql().side().close().__communicate__()

            client2.order().sql().exec("SELECT * FROM main").__communicate__()

            resp = client2.order().sql().lock().__communicate__()
            log_ok(resp)
            self.assertEqual(
                resp[0]["sql"]["lock"],
                client2.order().connection().id().__communicate__()[0]["connection"]["id"]
            )
            resp = client2.order().sql().arraysize().__communicate__()
            self.assertEqual(resp[0]["sql"]["arraysize"], 1)
            resp = client2.order().sql().arraysize(2).__communicate__()
            self.assertEqual(resp[0]["sql"]["arraysize"], 2)
            resp = client2.order().sql().description().__communicate__()
            self.assertEqual(resp[0]["sql"]["description"], [['x', None, None, None, None, None, None], ['y', None, None, None, None, None, None]])
            resp = client2.order().sql().lastrowid().__communicate__()
            self.assertEqual(resp[0]["sql"]["lastrowid"], 5)
            resp = client2.order().sql().rowcount().__communicate__()
            self.assertEqual(resp[0]["sql"]["rowcount"], -1)
            resp = client2.order().sql().rollback().__communicate__()
            resp = client2.order().sql().commit().__communicate__()

            client2.order().sql().force().exec("SELECT * FROM main").__communicate__()
            resp = client2.order().sql().sudo().fetchall().release(finally_=True).__communicate__()
            self.assertEqual(resp[0]["sql"]["fetch"], [])

            client.communicate([
                client.order().sql().release(),
                client.order().sql().exec("CREATE TABLE bytes (x BLOB)").release(),
            ])
            client.order().sql().exec("INSERT INTO bytes VALUES (?)", params=(22,)).release().__communicate__()
            client.order().sql().exec("INSERT INTO bytes VALUES (:x)", params={"x": 00}).release().__communicate__()
            client.order().sql().many("INSERT INTO bytes VALUES (?)", tb_params=(('b:' + base64.b64encode(b'bytes1').decode(),), ("t:string",))).release().__communicate__()
            with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
                client.order().sql().exec("INSERT INTO bytes VALUES (?)", tb_params=(base64.b64encode(b'bytes2').decode(),)).release().__communicate__()
            with self.assertRaises(wsqlite3.OrderError):
                ar.exception.raise_origin()

            client.order().sql().exec("SELECT * FROM bytes").__communicate__()
            self.assertEqual(
                client.order().sql().fetchone().__communicate__()[0]["sql"]["fetch"][0],
                22
            )
            self.assertEqual(
                client.order().sql().fetchone().__communicate__()[0]["sql"]["fetch"][0],
                0
            )
            self.assertEqual(
                base64.b64decode(client.order().sql().fetchone().__communicate__()[0]["sql"]["fetch"][0]),
                b'bytes1'
            )
            self.assertEqual(
                client.order().sql().fetchone().__communicate__()[0]["sql"]["fetch"][0],
                'string'
            )

            with self.assertRaises(queue.Empty) as ar:
                client.order().server().shutdown().__communicate__(False)

        except wsqlite3.baseclient.ClientError as e:
            e.raise_origin()

    def test_broadcast(self):

        server = wsqlite3.verbose_service.DebugServer("localhost", 9092, threads=2)
        server.start(wait_iterations=True)

        broadcast_comp: Callable[[dict], ...]

        class ClientConn(wsqlite3.baseclient.Connection):

            async def at_broadcast(self, broadcast: dict) -> None:
                broadcast_comp(broadcast)
                return await super().at_broadcast(broadcast)

        clients = [ClientConn("localhost", 9092) for i in range(3)]
        for client in clients:
            client.start()

        count = 0

        def _broadcast_comp(bc):
            nonlocal count, client_id
            count += 1
            log_ok(bc)
            self.assertEqual(bc, {"broadcast": 42, "from": client_id})

        broadcast_comp = _broadcast_comp

        for client in clients:
            client_id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
            client.order().broadcast(42).__communicate__()
            time.sleep(1)

        for client in clients:
            client_id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
            client.order().broadcast(42, True).__communicate__()
            time.sleep(1)

        self.assertEqual(count, 15)

        with self.assertRaises(queue.Empty) as ar:
            clients[0].order().server().shutdown().__communicate__(False)




