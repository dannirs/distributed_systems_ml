import socket
import threading
import os

# what to do if a file of the same name has already been saved by the server?

file_store = {}

def start_tcp_server():
    global latest_message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', 65432)
    print(f"Starting TCP server on {server_address[0]}:{server_address[1]}")
    server_socket.bind(server_address)
    server_socket.listen(5)
 
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

def handle_client(client_socket):
    try:
        print(f"Connection established with client")
        while True:
            method = client_socket.recv(4).decode('utf-8')
            if method == 'post':
                file_name_size = int.from_bytes(client_socket.recv(4), 'big')
                if file_name_size > 0:
                    file_name = client_socket.recv(file_name_size).decode('utf-8')
                    print("Server is receiving file from client: ", file_name)
                    if not os.path.exists("server_files"):
                        os.makedirs("server_files")
                    file_size = int.from_bytes(client_socket.recv(8), 'big')
                    file_path = f"server_files/{file_name}"
                    bytes_received = 0
                    with open(file_path, 'wb') as f:
                        while bytes_received < file_size:
                            file_data = client_socket.recv(1024)
                            if not file_data:
                                break
                            f.write(file_data)
                            bytes_received += len(file_data)
                    file_store[file_name] = file_path
                    f.close()
                    print("File retrieved.")
                    client_socket.send(b'File received and stored')
                else:
                    break
            elif method == 'get':
                file_name_size = int.from_bytes(client_socket.recv(4), 'big')
                if file_name_size > 0:
                    file_name = client_socket.recv(file_name_size).decode('utf-8')
                    if file_name in file_store:
                        file_path = file_store[file_name]
                        print("Server is retrieving file for client: ",file_name)
                        file_size = os.path.getsize(file_path)
                        client_socket.send(file_size.to_bytes(8, 'big'))
 
                        with open(file_path, 'rb') as f:
                            while True:
                                file_data = f.read(1024)
                                if not file_data:
                                    break
                                client_socket.send(file_data)
                        f.close()
                        print("File sent.")
                    else:
                        print("File not found: ", file_name)
                else:
                    break
            else:
                break

    finally:
        client_socket.close()

if __name__ == "__main__":
    start_tcp_server()