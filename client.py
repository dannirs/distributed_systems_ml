import socket
import os
import threading

def send_file(file_name, client_socket):
    # Send 'post' message type
    client_socket.send(b'post')
    
    # Send file name
    client_socket.send(file_name.encode())

    # Send the file content
    with open(file_name, 'rb') as f:
        while True:
            file_data = f.read(1024)
            if not file_data:
                break
            client_socket.send(file_data)

def get_file(file_name, client_socket):
    # Send 'get' message type
    client_socket.send(b'get')
    
    # Send file name to retrieve
    client_socket.send(file_name.encode())
    
    # Receive the file from the server
    with open(f'downloaded_{file_name}', 'wb') as f:
        while True:
            file_data = client_socket.recv(1024)
            if not file_data:
                break
            f.write(file_data)

def start_client(message):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server_address = ('localhost', 65432)  
    print(f"Connecting to {server_address[0]}:{server_address[1]}")
    client_socket.connect(server_address)
    
    try:
        # Example file sending (post)
        # file_name = "test.txt"
        print(f"Sending: {message}")
        client_socket.sendall(message.encode('utf-8'))
        
        data = client_socket.recv(1024)
        print(f"Received from server: {data.decode('utf-8')}")
        # send_file(file_name, client_socket)
        # print("Send file to server")
        
        # # Example file receiving (get)
        # get_file(file_name, client_socket)
        # print("Received file from server")
    
    finally:
        print("Closing connection")
        client_socket.close()

def test_connect_two_clients():
    thread1 = threading.Thread(target=start_client, args=("Hello from client 1!",))
    thread2 = threading.Thread(target=start_client, args=("Hello from client 2!",))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

if __name__ == "__main__":
    test_connect_two_clients()