#!/usr/bin/env python3
"""A TCP proxy with faults, for connection loss the way a network does it. tcpfault.py LISTEN_PORT TARGET_PORT CONTROL_FILE
reads the control file about every 100 ms. Its first word is the mode, an optional second word a new target port:
  pass       forward
  blackhole  sockets stay open, nothing moves in either direction, new connections are accepted and stay silent
  drop       like blackhole for the connections open at that moment, for good; new ones forward (a firewall that lost its state)
  reset      close every open connection at once, then forward
  refuse     close every connection, open or new
"""
import asyncio, sys

listen, target_port, control = int(sys.argv[1]), int(sys.argv[2]), sys.argv[3]
mode, conns = "pass", set()


class Conn:
    def __init__(self):
        self.dropped, self.writers = False, []

    def close(self):
        for w in self.writers:
            w.transport.abort()


async def pump(r, w, conn):
    try:
        while True:
            data = await r.read(65536)
            if not data:
                break
            while mode == "blackhole" or conn.dropped:
                await asyncio.sleep(0.05)
            w.write(data)
            await w.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        conn.close()


async def handle(r, w):
    if mode == "refuse":
        w.transport.abort()
        return
    conn = Conn()
    conn.writers.append(w)
    conns.add(conn)
    try:
        while mode == "blackhole":
            await asyncio.sleep(0.05)
        r2, w2 = await asyncio.open_connection("127.0.0.1", target_port)
        conn.writers.append(w2)
        await asyncio.gather(pump(r, w2, conn), pump(r2, w, conn))
    except OSError:
        pass
    finally:
        conn.close()
        conns.discard(conn)


async def watch():
    global mode, target_port
    while True:
        try:
            words = open(control).read().split()
        except OSError:
            words = []
        new = words[0] if words else "pass"
        if len(words) > 1:
            target_port = int(words[1])
        if new != mode:
            if new == "drop":
                for c in conns:
                    c.dropped = True
            if new in ("reset", "refuse"):
                for c in list(conns):
                    c.close()
            mode = new
        await asyncio.sleep(0.1)


async def main():
    server = await asyncio.start_server(handle, "127.0.0.1", listen)
    await asyncio.gather(server.serve_forever(), watch())


asyncio.run(main())
