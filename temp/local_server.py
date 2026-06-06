import socket
import os

SOCKET_PATH = '/tmp/my_unix_socket'
if os.path.exists(SOCKET_PATH):
    os.remove(SOCKET_PATH)
server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server.bind(SOCKET_PATH)
server.listen(1)
print(f'Server listening on {SOCKET_PATH}...')

try:
    while True:
        connection, client_address = server.accept()
        try:
            data = connection.recv(1024)
            if data:
                message = data.decode('utf-8')
                print(f'Received: {message}')
                response = f'Echo: {message}'.encode('utf-8')
                connection.sendall(response)
        finally:
            connection.close()
except KeyboardInterrupt:
    print('\nShutting server down.')
finally: 
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)
