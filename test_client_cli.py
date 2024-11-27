from __future__ import annotations

import asyncio
import json
import socket

import aioconsole
import wsdatautil

ws_stream_reader = wsdatautil.ProgressiveStreamReader("auto")
addr = ("localhost", 8881)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(addr)


async def connect():
    asyncio.get_running_loop().set_debug(True)
    r, w = await asyncio.open_connection(sock=sock)
    handshake = wsdatautil.HandshakeRequest()
    w.write(handshake.to_streamdata())
    await w.drain()
    handshake_data = await r.readuntil(b'\r\n\r\n')

    print("hs:", handshake_data)

    async def writer():
        while True:
            data = await aioconsole.ainput('{}?> ')
            try:
                obj = eval(data)
                f = wsdatautil.Frame(payload=json.dumps(obj).encode())
                d = f.to_streamdata()
                w.write(d)
                await w.drain()
            except Exception as e:
                print(e)

    async def reader():
        while True:
            print("...")
            var = 2
            while isinstance(var, int):
                try:
                    d = await r.readexactly(var)
                    print("d:", d)
                except Exception as e:
                    print("e:", e)
                    return
                var = ws_stream_reader.progressive_read(d)
            frame: wsdatautil.Frame = var
            print("f:", frame)

    await asyncio.gather(writer(), reader())

asyncio.run(connect())
