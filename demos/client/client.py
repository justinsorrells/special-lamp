import asyncio
from collections import deque
from enum import Enum
import json
import os
import socket
import struct
import time

SOCKET_PATH = "/tmp/chudmail"


class Status(Enum):
    """
    ENUM for UDPClient connection statuses
    """

    CONNECTED = "connected"
    DISCONNECTED = "disconnected"


class UDPClient:
    """
    UDPClient class for persistent connection with UDP server.
    """

    def __init__(self, addr):
        self.addr = addr  # (HOSTNAME, PORT)
        self.args = None
        self.cmd = None
        self.dropped = 0
        self.hostname = None
        self.last_ack = 0
        self.output = None  # dict[str, Any]
        self.packetsize = 1024
        self.schema = None
        self.sequence_no = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.status = Status.DISCONNECTED
        self.threshold = 5.0
        self.timestamp = time.monotonic()
        self.queue = deque()

    async def _connect(self):
        """
        Verify connection to server and update schema.
        """
        packet = {
            "type": "command",
            "sequence_no": self.sequence_no,
            "timestamp": time.monotonic(),
            "cmd": "info",
            "args": [],
        }
        await self._send(self._encode(packet))
        try:
            data, addr = await self._receive()
        except asyncio.TimeoutError:
            print(f"packet_no: {self.sequence_no} timed out")
            self.dropped += 1
            return None
        data = self._decode(data)
        if data.get("status_code") == 200:
            self.last_ack = data["sequence_no"]
            self.schema = data.get("result", {}).get("functions", None)
            self.status = Status.CONNECTED
            self.timestamp = data["timestamp"]

    def _decode(self, packet):
        return json.loads(packet.decode("utf-8"))

    def _encode(self, packet):
        return json.dumps(packet).encode("utf-8")

    def _next_sequence_no(self):
        self.sequence_no += 1
        return self.sequence_no

    async def _receive(self, timeout=0.5):
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.sock_recvfrom(self.socket, self.packetsize),
            timeout=timeout,
        )

    async def _send(self, packet):
        loop = asyncio.get_running_loop()
        try:
            await loop.sock_sendto(self.socket, packet, self.addr)
        except Exception:
            print(f"send to {self.addr} failed...")

    def enqueue_command(self, packet):
        self.queue.append(packet)

    async def send_command(self, timeout=0.5):
        self.set_command()
        if time.monotonic() - self.timestamp > self.threshold or self.schema is None:
            self.status = Status.DISCONNECTED
        if self.status == Status.DISCONNECTED:
            await self._connect()
            return
        seq = self._next_sequence_no()
        packet = {
            "type": "command",
            "sequence_no": seq,
            "timestamp": time.monotonic(),
            "cmd": self.cmd,
            "args": self.args,
        }
        await self._send(self._encode(packet))
        try:
            data, addr = await self._receive(timeout)
        except asyncio.TimeoutError:
            print(f"packet_no: {seq} timed out")
            self.dropped += 1
            return None
        data = self._decode(data)
        if data["sequence_no"] > self.last_ack:
            self.last_ack = data["sequence_no"]
            self.output = data
            self.timestamp = data["timestamp"]
        return self.output

    def set_command(self):
        if len(self.queue) <= 0:
            self.cmd = "info"
            self.args = []
            return
        cmd, args = self.queue.popleft()
        self.cmd = cmd
        self.args = args


def get_info(clients):
    res = {}
    for client in clients.values():
        res[client.hostname] = client.schema
    return res


async def handle_command(reader, writer, clients):
    try:
        data = await reader.readline()
        if not data:
            return
        message = json.loads(data.decode("utf-8"))
        client_id = int(message["client"])
        cmd = message["cmd"]
        args = message.get("args", [])
        if cmd == "boards":
            response = get_info(clients)
        elif client_id in clients:
            clients[client_id].enqueue_command((cmd, args))
            response = {"status": "accepted"}
        else:
            response = {"status": "rejected", "reason": "unknown client"}
        writer.write((json.dumps(response) + "\n").encode("utf-8"))
        await writer.drain()
    except Exception as e:
        response = {"status": "error", "reason": str(e)}
        writer.write((json.dumps(response) + "\n").encode("utf-8"))
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


async def command_updater(clients, stop):
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)

    server = await asyncio.start_unix_server(
        lambda reader, writer: handle_command(reader, writer, clients),
        path=SOCKET_PATH,
    )

    async with server:
        while not stop.is_set():
            await asyncio.sleep(0.1)

    server.close()
    await server.wait_closed()


async def flood(client, stop, hz=50):
    interval = 1.0 / hz
    while not stop.is_set():
        res = await client.send_command(timeout=0.02)
        print(f"{client.hostname}: {res}")
        await asyncio.sleep(interval)


async def main():
    start = time.monotonic()
    stop = asyncio.Event()
    clients = {
        1: UDPClient(("127.0.0.1", 6767)),
    }
    for idx, client in enumerate(clients.values()):
        client.enqueue_command(("info", []))
        client.hostname = idx + 1

    await asyncio.gather(
        *(flood(client, stop, hz=1) for client in clients.values()),
        command_updater(clients, stop),
    )
    print(time.monotonic() - start)


if __name__ == "__main__":
    asyncio.run(main())
