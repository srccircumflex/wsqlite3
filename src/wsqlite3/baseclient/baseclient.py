from __future__ import annotations

import asyncio
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
            await self.at_broadcast(response)
        else:
            self.response_queue.put(response)

    async def loop(self):
        with self.sock:
            reader, _ = await asyncio.open_connection(sock=self.sock)
            self.async_reader = reader
            while True:
                await self.read_iteration()

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
            if e.errno != 106:
                raise

    def run(self):
        if not self.is_alive():
            self._connect()
        asyncio.run(self.loop())

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
        self.sock.send(
            wsdatautil.Frame(
                self.serialize_output(obj),
                wsdatautil.OPCODES.BINARY,
                mask=None  # against RFC6455
            ).to_streamdata()
        )

    def recv_obj(self, block: bool = True, timeout: float | None = None) -> dict:
        return self.response_queue.get(block, timeout)

    def communicate(self, obj: dict | object, block: bool = True, timeout: float | None = None) -> list[dict]:
        if isinstance(obj, list):
            for i in range(len(obj)):
                order = obj[i]
                if isinstance(order, orderutil._OrderSection) and not isinstance(order, orderutil.Order):
                    obj[i] = order.__root__
        self.send_obj(obj)
        response = self.recv_obj(block, timeout)
        orders = response.get("orders")
        c = response.get("errors")
        if c == -1:
            raise orderutil.FatalError(-1, response["error"])
        elif c not in (0, 2):
            raise orderutil.ClientError(c, response["error"])
        return orders

    async def at_broadcast(self, broadcast: dict) -> None:
        print(broadcast)

    def order(self, block: bool = True, timeout: int | None = None) -> orderutil.Order:
        return orderutil.Order(self, block, timeout)
