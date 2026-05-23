import socket

def hello(name):
    print(f'Hello, {name}')
    return f'Hello, {name}'

def parse_cmd(cmd):
    return cmd.split(',')

if __name__ == "__main__":
    available = {
        "hello": {
            "function"=hello,
            "args"=["str"],
        },        
    }
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    serversocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    serversocket.bind(('0.0.0.0', 6767))
    print(serversocket)
    while True:
        data, addr = serversocket.recvfrom(1024)
        parsed_cmd = data.decode('utf-8').strip().lower().split(',')
        cmd = parsed_cmd[0]
        if cmd in available:
            res = available[cmd]().encode('utf-8')
            serversocket.sendto(res, addr)
        print(cmd)
