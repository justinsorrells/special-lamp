import socket
import time

HOST = 'put hostname here'
PORT = 6767

if __name__ == "__main__":
    while True:
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print("sending hello")
        clientSocket.sendto(b"hello", (HOST, PORT))
        dataRec = clientSocket.recvfrom(1024)
        print("data recieved: ", dataRec)
        time.sleep(1)
    
