import socket
import os
import threading

def send_file(file_name, client_socket):
    client_socket.send(b'post')
    file_name_bytes = file_name.encode('utf-8')
    file_name_size = len(file_name_bytes)
    client_socket.send(file_name_size.to_bytes(4, 'big')) 
    client_socket.send(file_name_bytes)
    print("Client is sending file to server: ", file_name)
    file_size = os.path.getsize(file_name)
    client_socket.send(file_size.to_bytes(8, 'big'))
    with open(file_name, 'rb') as f:
        while True:
            file_data = f.read(1024)
            if not file_data:
                break
            client_socket.send(file_data)
    f.close()
    print("File sent.")

def get_file(file_name, client_socket):
    client_socket.send(b'get')

    file_name_bytes = file_name.encode('utf-8')
    file_name_size = len(file_name_bytes)
    client_socket.send(file_name_size.to_bytes(4, 'big'))  
    client_socket.send(file_name_bytes)
    print("Client is getting file from server: ", file_name)
    file_size = int.from_bytes(client_socket.recv(8), 'big')

    bytes_received = 0
    with open(f'downloaded_{file_name}', 'wb') as f:
        while bytes_received < file_size:
            file_data = client_socket.recv(1024)
            if not file_data:
                break
            f.write(file_data)
            bytes_received += len(file_data)
    f.close()
    print("File retrieved.")

def start_client(file_name):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server_address = ('localhost', 65432)  
    print(f"Connecting to {server_address[0]}:{server_address[1]}")
    client_socket.connect(server_address)
    
    try:
        send_file(file_name, client_socket)
        response = client_socket.recv(1024)
        print(f"Server response: {response}")
        get_file(file_name, client_socket)

    finally:
        print("Closing connection")
        client_socket.close()

def test_multiple_clients():
    thread1 = threading.Thread(target=start_client, args=("username.csv",))
    thread2 = threading.Thread(target=start_client, args=("username-password-recovery-code.csv",))
    thread3 = threading.Thread(target=start_client, args=("username-password-recovery-code.csv",))

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join() 
    thread3.join()

if __name__ == "__main__":
    test_multiple_clients()