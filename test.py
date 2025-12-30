from __future__ import annotations

import base64
import pathlib
import sys
from warnings import filterwarnings

try:
    sys.path.insert(0, str(pathlib.Path(__file__).parent))
except Exception:
    raise

import queue
import socket
import sqlite3
import threading
from typing import Callable
import time
import random

import unittest

from src import wsqlite3

filterwarnings(
    'ignore',
    message=r'^unclosed.*',
    module="base_events",
    category=ResourceWarning
)


def log_ok(*msg: object):
    print("", end="", flush=True)
    print("\x1b[32mOK:", *msg, "\x1b[m", flush=True)


n_threads = 1
n_concurrent = 10
P = threading.Thread
# P = multiprocessing.Process


def _intro():
    log_ok(f"""[TEST CONFIG]
      n_threads    (Server.threads) = {n_threads}
      n_concurrent (concurrency)    = {n_concurrent}
      P            (concurrency)    = {P}
      
known warnings:
    - "ResourceWarning: unclosed ..." (conflict with test environment)
    - "ResourceWarning: Enable tracemalloc to get the object allocation traceback" (conflict with test environment)
    - "Task was destroyed but it is pending!" (shutdown bug)
    - "concurrent.futures._base.CancelledError" (shutdown bug)
    """.strip())

    for _ in range(3):
        time.sleep(.5)
        print(".", end="", flush=True)
    print(flush=True)


_intro()


class TestConnectionLimit(unittest.TestCase):

    def test_single(self):
        port = 9990
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), connections_per_thread=1)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()
        resp = client.order().ping().__communicate__()

        client2 = wsqlite3.baseclient.Connection("localhost", port)
        with self.assertRaises(ConnectionResetError) as ar:
            client2.connect()
        client2.sock.close()

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_two_threads(self):
        port = 9991
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=2, connections_per_thread=1)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()
        resp = client.order().ping().__communicate__()

        client2 = wsqlite3.baseclient.Connection("localhost", port)
        client2.start()
        resp2 = client.order().ping().__communicate__()

        client3 = wsqlite3.baseclient.Connection("localhost", port)
        with self.assertRaises(ConnectionResetError) as ar:
            client3.connect()
        client3.sock.close()

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_no_limit(self):
        for t in (4, 1):
            port = 9090 + t
            server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=t, connections_per_thread=0)
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

    def test_autoclose(self):
        port = 9994
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        comp = self.assertEqual
        cancel = True

        try:

            class ClientConn(wsqlite3.baseclient.Connection):

                async def at_autoclose(self, request: dict) -> None:
                    log_ok(request)
                    if cancel:
                        log_ok('****')
                        log_ok(self.order().autoclose().cancel().__communicate__())
                        log_ok('++++')
                    comp(request, {"autoclose": {
                        "trigger": trigger,
                        "reason": 'destroy ordered',
                        "current_conn_value": 1,
                    }})
                    await super().at_broadcast(request)

            client1 = ClientConn("localhost", port)
            client1.start()

            client2 = ClientConn("localhost", port)
            client2.start()

            client3 = ClientConn("localhost", port)
            client3.start()

            trigger = client1.order().connection().id().__communicate__()[0]["connection"]["id"]

            client2.order().autoclose().value("request").__communicate__()
            client3.order().autoclose().value("request").__communicate__()

            client1.order().connection().destroy().__communicate__()

            time.sleep(2)

            log_ok(client2.order().ping().__communicate__())
            log_ok(client3.order().ping().__communicate__())

            trigger = client3.order().connection().id().__communicate__()[0]["connection"]["id"]
            log_ok(client2.order().connection().get(trigger).destroy().__communicate__())

            time.sleep(2)

            log_ok(client2.order().ping().__communicate__())

            client4 = ClientConn("localhost", port)
            client4.start()
            client4.order().autoclose().value("request").__communicate__()

            trigger = client2.order().connection().id().__communicate__()[0]["connection"]["id"]
            client2.order().autoclose().trigger().__communicate__()

            time.sleep(2)

            cancel = False
            client2.order().autoclose().trigger().__communicate__()

            time.sleep(2)

            with self.assertRaises(OSError) as ar:
                client2.order().__communicate__()

        except wsqlite3.baseclient.ClientError as e:
            e.raise_origin()


class TestOrders(unittest.TestCase):

    def test_ping(self):
        port = 9995
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        resp = client.order().ping().__communicate__()[0]

        self.assertTrue(isinstance(resp["ping"]["pong"], int))

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test__exec(self):
        port = 9996
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        resp = client.order().exec("result = print(self.current_connection.thread.id) or 42").__communicate__()[0]

        self.assertEqual(resp["_exec"]["result"], 42)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test__getattr(self):
        port = 9997
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        resp = client.order().getattr("server", "socket.getsockopt", ((socket.SOL_SOCKET, socket.SO_REUSEADDR), {})).__communicate__()[0]

        self.assertEqual(resp["_getattr"]["result"], 1)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_connection(self):
        port = 9998
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=2)
        server.start(wait_iterations=True)

        client1 = wsqlite3.baseclient.Connection("localhost", port)
        client1.start()

        resp = client1.order().connection().id().__communicate__()[0]
        c1_id = resp["connection"]["id"]
        log_ok(resp)

        desc1 = {"c1": 1, "x": 0}
        resp = client1.order().connection().description_set(desc1).__communicate__()[0]
        resp = client1.order().connection().description().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        desc1.pop("x")
        resp = client1.order().connection().description_pop("x").__communicate__()[0]
        self.assertEqual(resp["connection"]["description.pop"], 0)
        resp = client1.order().connection().description().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        desc_upd = {"y": 1}
        desc1 |= desc_upd
        resp = client1.order().connection().description_update(desc_upd).__communicate__()[0]
        resp = client1.order().connection().description().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        resp = client1.order().thread().properties().__communicate__()[0]
        log_ok(resp)
        t1_prop = resp["thread"]["properties"]

        resp = client1.order().connection().properties().__communicate__()[0]
        self.assertEqual(resp["connection"]["properties"]["description"], desc1)
        self.assertEqual(resp["connection"]["properties"]["thread"], t1_prop)
        self.assertEqual(resp["connection"]["properties"]["id"], c1_id)
        self.assertEqual(resp["connection"]["properties"]["address"], list(client1.sock.getsockname()))

        client2 = wsqlite3.baseclient.Connection("localhost", port)
        client2.start()

        desc2 = {"c2": 2, "x": 0}
        resp = client2.order().connection().description_set(desc2).__communicate__()[0]

        resp = client2.order().connection().get(c1_id).description().__communicate__()[0]
        self.assertEqual(resp["connection"]["description"], desc1)

        resp = client2.order().connection().description().__communicate__()[0]
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

        client1.order().connection().get(c2_id).destroy().__communicate__()
        time.sleep(.5)
        with self.assertRaises(OSError) as ar:
            client2.order().__communicate__(False)

        with self.assertRaises(queue.Empty) as ar:
            client1.order().server().shutdown().__communicate__(False)

    def test_server(self):
        port = 10999
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        resp = client.order().server().connections().__communicate__()[0]
        log_ok(resp)

        resp = client.order().server().threads().__communicate__()[0]
        log_ok(resp)

        with self.assertRaises(queue.Empty) as ar:
            client.order().server().shutdown().__communicate__(False)

    def test_thread(self):
        port = 10000
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=2)
        server.start(wait_iterations=True)

        broadcast_comp: Callable[[dict], ...]

        class ClientConn(wsqlite3.baseclient.Connection):

            async def at_broadcast(self, broadcast: dict) -> None:
                broadcast_comp(broadcast)
                return await super().at_broadcast(broadcast)

        client1 = ClientConn("localhost", port)
        client1.start()

        client2 = ClientConn("localhost", port)
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
        port = 10001
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
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

            client2 = wsqlite3.baseclient.Connection("localhost", port)
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
        port = 10002
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        broadcast_comp: Callable[[dict], ...]

        class ClientConn(wsqlite3.baseclient.Connection):

            async def at_broadcast(self, broadcast: dict) -> None:
                broadcast_comp(broadcast)
                self.order().ping().__communicate__()
                return await super().at_broadcast(broadcast)

        clients = [ClientConn("localhost", port) for i in range(3)]
        for client in clients:
            client.start()

        count = 0

        def _broadcast_comp(bc):
            nonlocal count, client_id
            count += 1
            log_ok(bc)
            self.assertEqual(bc, {"broadcast": 42, "from": client_id})

        broadcast_comp = _broadcast_comp

        client = clients[0]
        client_id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
        client.order().broadcast(42).__communicate__()

        time.sleep(1)

        client = clients[1]
        client_id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
        client.order().broadcast(42, True).__communicate__()

        time.sleep(1)

        self.assertEqual(count, 5)

        with self.assertRaises(queue.Empty) as ar:
            clients[0].order().server().shutdown().__communicate__(False)

    def test_autoclose(self):
        port = 10010
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        config = client.order().autoclose().config().__communicate__()[0]["autoclose"]["config"]
        log_ok(config)
        client.order().autoclose()\
            .config_block(False)\
            .config_request(0)\
            .config_wait_response(-1.1)\
            .config_wait_close(1)\
            .config_force_shutdown(True)\
            .config_sql_commit(1)\
            .__communicate__()

        self.assertEqual(
            client.order().autoclose().config().__communicate__()[0]["autoclose"]["config"],
            {'block': False, 'request': 0, 'wait_response': -1.1, 'wait_close': 1, 'force_shutdown': True, 'sql_commit': 1}
        )

        client.order().autoclose().value("block").__communicate__()
        client.order().autoclose().value("request").__communicate__()
        client.order().autoclose().value(0).__communicate__()

        with self.assertRaises(wsqlite3.baseclient.ClientError) as ar:
            client.order().autoclose().value("\0").__communicate__()
        with self.assertRaises(wsqlite3.OrderError):
            ar.exception.raise_origin()

        client.order().server().shutdown().__communicate__()


class TestConcurrency(unittest.TestCase):

    def test_concurrency(self):
        port = 10003
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        def sleep():
            time.sleep(random.random() + random.random())

        server.add_database(None, ':memory:')
        server.get_database(None).connection.execute("CREATE TABLE main (x INT, y INT)")

        ids = list()

        def concurrent():
            client = wsqlite3.baseclient.Connection("localhost", port)
            client.start()
            sleep()
            _id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
            ids.append(_id)
            sleep()
            r = random.random()
            orders = (
                client.order().connection().id(),
                client.order().broadcast(42, True),
                client.order().sql().exec("INSERT INTO main VALUES (11, ?)", params=(22,)).release(),
                client.order().connection().description_update({"r": r}),
                client.order().sql().exec("INSERT INTO main VALUES (99, :x)", params={"x": 00}).release(),
                client.order().sql().exec("SELECT * FROM main").release(),
                client.order().connection().description_update({"id": _id}),
                client.order().connection().get(__id := random.choice(ids)).properties(),
                client.order().connection().get(__id).properties(),
                client.order().connection().get(random.choice(ids)).properties(),
                client.order().connection().get(__id).properties(),
                [
                    client.order().connection().get(random.choice(ids), "session"),
                    client.order().ping()
                ],
                [
                    client.order().connection().get(random.choice(ids), "response"),
                    client.order().ping()
                ],
                [
                    client.order().connection().get(random.choice(ids), "session"),
                    client.order().ping()
                ],
                client.order().connection().get(random.choice(ids)).properties(),
                client.order().connection().properties(),
            )
            log_ok(_id, "start")
            for order in orders:
                sleep()
                client.send_obj(order)
            client.send_obj(client.order(flag=True).connection().description())

            while True:
                res = client.recv_obj()
                if True in res["flags"]:
                    break

            self.assertEqual(
                res["orders"][0]["connection"]["description"],
                {"r": r, "id": _id}
            )
            log_ok(_id, "end")

        procs = [P(target=concurrent, daemon=True) for _ in range(n_concurrent)]

        for proc in procs:
            proc.start()

        for proc in procs:
            proc.join()

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()

        client.order().server().shutdown().__communicate__()

    def test_concurrency2(self):
        port = 10004
        server = wsqlite3.verbose_service.DebugServer(("localhost", port), threads=n_threads)
        server.start(wait_iterations=True)

        def sleep():
            time.sleep(random.random() + random.random())

        server.add_database(None, ':memory:')
        server.get_database(None).connection.execute("CREATE TABLE main (x INT, y INT)")

        _id = None

        def concurrent():
            nonlocal _id
            client = wsqlite3.baseclient.Connection("localhost", port)
            client.start()
            client.order().connection().description_set({"1": 1}).__communicate__()
            _id = client.order().connection().id().__communicate__()[0]["connection"]["id"]
            client.order().autoclose().trigger().__communicate__()

        client = wsqlite3.baseclient.Connection("localhost", port)
        client.start()
        client.order().autoclose().value("request").__communicate__()

        proc = threading.Thread(target=concurrent, daemon=True)
        proc.start()

        while not _id:
            pass

        time.sleep(.5)
        self.assertEqual(
            client.order().connection().get(_id, "session").description().__communicate__()[0]["connection"]["description"],
            {"1": 1}
        )

        while proc.is_alive():
            pass

        client.order().server().shutdown().__communicate__()


if __name__ == '__main__':
    unittest.main()
