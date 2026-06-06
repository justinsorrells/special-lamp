import asyncio
import json
import os
import socket

SOCKET_PATH = '/tmp/chudmail'

if not os.path.exists(SOCKET_PATH):
    raise FileNotFoundError('Error local server not running')

client = asyncio.open_unix_connection(SOCKET_PATH)
client.connect(SOCKET_PATH)
mesg = {
    'client': 1,
    'cmd': 'boards',
    'args': [],
}
client.send((json.dumps(mesg)+"\n").encode('utf-8'))
buff = client.recv()
print(json.loads(buff.decode('utf-8')))
