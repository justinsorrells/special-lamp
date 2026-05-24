import inspect
import json
import socket
import time

DEBUG = 1


def hello(name: str) -> str:
    print(f"Hello, {name}")
    return f"Hello, {name}"


def parse_data(packet):
    obj = json.loads(packet.decode("utf-8"))
    return obj


schema = {
    "info": {
        "function": None,
        "args": [],
    },
    "hello": {
        "function": hello,  # type: ignore[syntax]
        "args": ["str"],
    },
}

#timestamps
#sequence_number
#status_code
#cmd_output
#error

def evaluate_command(cmd, args):
    if cmd not in schema:
        raise ValueError(f"{cmd} no in schema")
    func = schema[cmd]["function"]
    sig = inspect.signature(func)
    try:
        sig.bind(*args)
    except TypeError as e:
        raise ValueError(f"bad arguments for {cmd}: {e}")
    return func(*args)

def generate_response(packet):
    response = {
            "type": "response",
            "timestamp": time.time(),
            "sequence_no": packet["sequence_no"],
            "status_code": 500,
            "result": None,
            "error": None,
            }
    try:
        response["result"] = evaluate_command(packet["cmd"], packet.get("args", []))
        response["status_code"] = 200
    except ValueError as e:
        response["error"] = f'error: {e}\n'
        response["status_code"] = 500
    return response

def info():
    return json.dumps(schema)


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
            if packet["type"] == "command":
                response = generate_response(packet)
            else:
                response = {
                    "type": "response",
                    "timestamp": time.time(),
                    "sequence_no": packet.get("sequence_no"),
                    "status_code": 400,
                    "result": None,
                    "error": "unsupported packet type",
                }
        except Exception as e:
            response = {
                "type": "response",
                "timestamp": time.time(),
                "sequence_no": packet["sequence_no"],
                "status_code": 500,
                "result": None,
                "error": f'bad packet {e}',
            }
        serversocket.sendto(json.dumps(response).encode("utf-8"),  addr)

