import inspect
import json
import socket

DEBUG = 1


def hello(name: str) -> str:
    print(f"Hello, {name}")
    return f"Hello, {name}"


def parse_data(packet):
    obj = json.loads(packet)
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
    sig = inspect.signature(cmd)
    try:
        sig.bind(*args)
    except TypeError as e:
        raise ValueError(f"bad arguments for {cmd}: {e}")
    return func(*args)

def generate_response(packet):
    pass 

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
        packet = parse_data(data)
        if packet["type"] == "command":
            response = generate_response(packet)
            serversocket.sendto(response, addr)


