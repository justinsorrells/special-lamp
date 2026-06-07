import inspect
import json
import socket
import time
from typing import Any, TypedDict, Callable

DEBUG = 1
HOSTNAME = "justin"

class CommandMeta(TypedDict):
    function: Callable[...,Any]
    args: list[str]


def hello(name: str) -> str:
    print(f"Hello, {name}")
    return f"Hello, {name}"


def parse_data(packet: bytes) -> dict[str, Any]:
    return json.loads(packet.decode('utf-8'))    

schema: dict[str, CommandMeta] = {
    "info": {
        "function": lambda: None,
        "args": [],
    },
    "hello": {
        "function": hello,  # type: ignore[syntax]
        "args": ["str"],
    },
}

def generate_public_schema():
    return { func_name: schema[func_name]['args'] for func_name in schema.keys() }

def evaluate_command(cmd: str, args: list[Any]) -> Any:
    if cmd not in schema:
        raise ValueError(f"{cmd} not in schema")
    func = schema[cmd]["function"]
    try:
        sig = inspect.signature(func)
        sig.bind(*args)
    except TypeError as e:
        raise ValueError(f"bad arguments for {cmd}: {e}") from e
    return func(*args)

def generate_response(packet):
    if not isinstance(packet, dict):
        raise ValueError('malformed packet')
    response = {
            "type": "response",
            "timestamp": packet.get("timestamp", -1),
            "sequence_no": packet.get("sequence_no", -1),
            "status_code": 500,
            "result": None,
            "error": None,
            }
    cmd = packet.get("cmd")
    if not isinstance(cmd, str):
        raise ValueError("invalid or missing cmd")
    args = packet.get("args", [])
    if not isinstance(args, list):
        raise ValueError("invalid or missing args") 
    try:
        response["result"] = evaluate_command(cmd, args)
        response["status_code"] = 200
    except ValueError as e:
        response["error"] = f'error: {e}\n'
        response["status_code"] = 500
    return response

def info() -> dict[str, list[Any]]:
    return { 
            "functions": generate_public_schema(),
            "hostname": HOSTNAME,
    }

if __name__ == "__main__":
    schema["info"]["function"] = info  # inject info command into schema
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(("0.0.0.0", 6767))
    if DEBUG == 1:
        print(serversocket)
    while True:
        data, addr = serversocket.recvfrom(1024)
        try:
            packet = parse_data(data)
            if packet.get("type") == "command":
                print(packet['cmd'])
                response = generate_response(packet)
            else:
                response = {
                    "type": "response",
                    "timestamp": time.time(),
                    "sequence_no": packet.get("sequence_no", -1),
                    "status_code": 400,
                    "result": None,
                    "error": "unsupported packet type",
                }
        except Exception as e:
            response = {
                "type": "response",
                "timestamp": time.time(),
                "sequence_no": -1,
                "status_code": 500,
                "result": None,
                "error": f'bad packet {e}',
            }
        try:
            serversocket.sendto(json.dumps(response).encode("utf-8"),  addr)
        except Exception:
            continue

