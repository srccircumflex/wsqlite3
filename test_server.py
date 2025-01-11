import sqlite3

from src.wsqlite3.verbose_service import VerboseServer, DebugServer, Server

addr = ("localhost", 8881)

server = DebugServer(
    addr,
    threads=0,
    connections_per_thread=0
)
server.add_database(
    None,
    sqlite3.connect(":memory:", check_same_thread=False)
)
server.add_database(
    2,
    ":memory:"
)
try:
    server.add_database(
        None,
        ":memory:"
    )
except Exception as e:
    print(e)
else:
    raise Exception

server.start()
