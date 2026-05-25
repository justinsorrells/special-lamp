# import asyncio
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
        self.last_ack = 0
        self.addr = addr # (HOSTNAME, PORT)
        self.hostname = None
        self.output = None # dict[str, Any]
        self.packetsize = 1024
        self.schema = None
        self.sequence_no = 0
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.status = Status.DISCONNECTED
        self.threshold = 5.0
        self.timestamp = time.monotonic()

    #        self.socket.setblocking(False)

    def _connect(self):
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
        self._send(self._encode(packet))
        data, addr = self._receive()
        data = self._decode(data)
        if data.get("status_code") == 200:
            self.last_ack = data["sequence_no"]
            self.schema = data.get("result", None)
            self.status = Status.CONNECTED
            self.timestamp = data["timestamp"]

    def _decode(self, packet):
        return json.loads(packet.decode("utf-8"))

    def _encode(self, packet):
        return json.dumps(packet).encode("utf-8")

    def _next_sequence_no(self):
        self.sequence_no += 1
        return self.sequence_no

    def _receive(self, timeout=.5):
        self.socket.settimeout(timeout)
        return self.socket.recvfrom(self.packetsize)

    def _send(self, packet):
        self.socket.sendto(packet, self.addr)

    def send_command(self, cmd, args=None, timeout=0.5):
        if time.monotonic() - self.timestamp > self.threshold or self.schema == None:
            self.status = Status.DISCONNECTED
        if self.status == Status.DISCONNECTED:
            self._connect()
            return
        args = args or []
        seq = self._next_sequence_no()
        packet = {
            "type": "command",
            "sequence_no": seq,
            "timestamp": time.monotonic(),
            "cmd": cmd,
            "args": args,
        }
        self._send(self._encode(packet))
        data, addr = self._receive(timeout)
        self.socket.settimeout(None)
        data = self._decode(data)
        if data["sequence_no"] > self.last_ack:
            self.last_ack = data["sequence_no"]
            self.output = data
            self.timestamp = data["timestamp"]
        return self.output


if __name__ == "__main__":
    client = UDPClient(("127.0.0.1", 6767))
    while True:
        cmd = input("enter command: ")
        args = input("enter args: ")
        args = args.split()
        res = client.send_command(cmd, args)
        print(res)
