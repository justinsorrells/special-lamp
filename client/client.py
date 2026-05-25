import asyncio
from enum import Enum
import json
import socket
import time


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
        self.cmd = None
        self.args = None
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
            print(f'packet_no: {self.sequence_no} timed out')
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
        await loop.sock_sendto(self.socket, packet, self.addr)

    async def send_command(self, timeout=0.5):
        if time.monotonic() - self.timestamp > self.threshold or self.schema == None:
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
            print(f'packet_no: {seq} timed out')
            return None
        data = self._decode(data)
        if data["sequence_no"] > self.last_ack:
            self.last_ack = data["sequence_no"]
            self.output = data
            self.timestamp = data["timestamp"]
        return self.output

    def set_command(self, cmd, args=None):
        self.cmd = cmd
        self.args = args


async def command_updater(clients):
    while True:
        target = await asyncio.to_thread(input, "client id> ")
        cmd = await asyncio.to_thread(input, "cmd> ")
        args = await asyncio.to_thread(input, "args> ")
        try:
            client_id = int(target)
            client = clients[client_id]
        except (ValueError, KeyError):
            print("invalid client id")
            continue
        client.set_command(cmd, args.split())


async def flood(client, hz=50):
    interval = 1.0 / hz
    while True:
        res = await client.send_command(timeout=0.02)
        print(f'{client.hostname}: {res}')
        await asyncio.sleep(interval)


async def main():
    clients = {
        1: UDPClient(("127.0.0.1", 6767)),
        2: UDPClient(("127.0.0.1", 6768)),
        3: UDPClient(("127.0.0.1", 6767)),
    }
    for idx,client in enumerate(clients.values()):
        client.set_command("info", [])
        client.hostname = idx+1

    await asyncio.gather(
        *(flood(client, hz=10) for client in clients.values()),
        command_updater(clients),
    )


if __name__ == "__main__":
    asyncio.run(main())
