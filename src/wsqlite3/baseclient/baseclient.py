from __future__ import annotations

import asyncio
import errno
import json
import queue
import socket
import threading
from collections.abc import Sequence, Mapping, Iterable

import wsdatautil

from . import orderutil


class Connection(threading.Thread):
    server_addr: tuple[str, int]
    sock: socket.socket
    async_reader: asyncio.StreamReader
    response_queue: queue.Queue[dict]
    ws_stream_reader: wsdatautil.ProgressiveStreamReader

    def __init__(
            self,
            server_host: str,
            server_port: int,
    ):
        threading.Thread.__init__(self)
        self.server_addr = (server_host, server_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.response_queue = queue.Queue()
        self.ws_stream_reader = wsdatautil.ProgressiveStreamReader(False)

    async def read_one_frame(self) -> wsdatautil.Frame:
        """Read one ws frame."""
        while True:
            var: int = 2
            while isinstance(var, int):
                var = self.ws_stream_reader.progressive_read(
                    await self.async_reader.readexactly(var)
                )
            var: wsdatautil.Frame
            match var.opcode:
                case wsdatautil.OPCODES.CLOSE:
                    pass
                case wsdatautil.OPCODES.PING:
                    pass
                case _:
                    return var

    async def read_frames_until_fin(self) -> list[wsdatautil.Frame]:
        """Read ws frames until a fin flag is reached."""
        frames: list[wsdatautil.Frame] = list()
        while not frames or not frames[-1].fin:
            frames.append(await self.read_one_frame())
        return frames

    async def read_iteration(self) -> None:
        frames = await self.read_frames_until_fin()
        payload = bytes().join(f.payload for f in frames)
        response = self.deserialize_input(payload)
        if response.get("broadcast"):
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, args=()).start()
            asyncio.run_coroutine_threadsafe(self.at_broadcast(response), loop).add_done_callback(
                lambda _: loop.call_soon_threadsafe(loop.stop, )
            )
        elif response.get("autoclose"):
            loop = asyncio.new_event_loop()
            threading.Thread(target=loop.run_forever, args=()).start()
            asyncio.run_coroutine_threadsafe(self.at_autoclose(response), loop).add_done_callback(
                lambda _: loop.call_soon_threadsafe(loop.stop, )
            )
        else:
            self.response_queue.put(response)

    async def read_loop(self):
        try:
            while True:
                await self.read_iteration()
        except asyncio.IncompleteReadError:
            self.response_queue.put(dict())

    async def main(self):
        with self.sock:
            reader, _ = await asyncio.open_connection(sock=self.sock)
            self.async_reader = reader
            await self.read_loop()

    def connect(self) -> bytes:
        handshake = wsdatautil.HandshakeRequest()
        self.sock.connect(self.server_addr)
        self.sock.send(handshake.to_streamdata())
        handshake_res = b''
        while not handshake_res.endswith(b'\r\n\r\n'):
            handshake_res += self.sock.recv(32)
        return handshake_res

    def _connect(self) -> bytes:
        try:
            return self.connect()
        except OSError as e:
            if e.errno != errno.EISCONN:  # Transport endpoint is already connected
                raise

    def run(self):
        if not self.is_alive():
            self._connect()
        asyncio.run(self.main())

    def start(self):
        self._connect()
        super().start()

    def json_default(self, obj: object):
        if isinstance(obj, orderutil._OrderSection):
            return obj.__data__
        elif isinstance(obj, Sequence):
            return list(obj)
        elif isinstance(obj, Mapping):
            return dict(obj)
        elif isinstance(obj, Iterable):
            return list(obj)
        else:
            return repr(obj)

    def deserialize_input(self, payload: bytes) -> dict | list[dict]:
        """deserialize payload from stream"""
        return json.loads(payload)

    def serialize_output(self, obj: dict | object) -> bytes:
        """serialize object for stream"""
        return json.dumps(obj, default=self.json_default).encode()

    def send_obj(self, obj: dict | object):
        if isinstance(obj, list):
            for i in range(len(obj)):
                order = obj[i]
                if isinstance(order, orderutil._OrderSection) and not isinstance(order, orderutil.Order):
                    obj[i] = order.__root__
        elif isinstance(obj, orderutil._OrderSection) and not isinstance(obj, orderutil.Order):
            obj = obj.__root__
        self.sock.sendall(
            wsdatautil.Frame(
                self.serialize_output(obj),
                wsdatautil.OPCODES.BINARY,
                mask=None  # against RFC6455
            ).to_streamdata()
        )

    def recv_obj(self, block: bool = True, timeout: float | None = None) -> dict:
        return self.response_queue.get(block, timeout)

    def communicate(self, obj: dict | object, block: bool = True, timeout: float | None = None) -> list[dict]:
        self.send_obj(obj)
        response = self.recv_obj(block, timeout)
        orders = response.get("orders")
        c = response.get("errors")
        if c == -1:
            raise orderutil.FatalError(-1, response["error"])
        elif c not in (0, 2, None):
            raise orderutil.ClientError(c, response["error"])
        return orders

    async def at_broadcast(self, broadcast: dict) -> None:
        print(broadcast)

    async def at_autoclose(self, request: dict) -> None:
        print(request)

    def order(self, flag=None, block: bool = True, timeout: int | None = None) -> orderutil.Order:
        return orderutil.Order(self, block, timeout, flag)

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sock.close()
