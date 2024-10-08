import socket
import os
import threading
from message import message
import json

def send_file(file_name, client_socket):
    file_name_bytes = file_name
    file_name_size = len(file_name_bytes)
    file_size = os.path.getsize(file_name)
    packet = message(
        method="SEND_FILE", 
        source_port=1234, 
        destination_port=5678, 
        header_list={"file_name_size": file_name_size, "file_name_bytes": file_name_bytes, "file_size": file_size}, 
        file=file_name
    )
    headers, payload = packet.process_request()
    client_socket.send(headers)
    for i in range(len(payload)):
        client_socket.send(payload[i])
    print("File sent.")

def get_file(file_name, client_socket):
    file_name_bytes = file_name
    file_name_size = len(file_name_bytes)
    packet = message(
        method="GET_FILE", 
        source_port=1234, 
        destination_port=5678, 
        header_list={"file_name_size": file_name_size, "file_name_bytes": file_name_bytes}
    )
    headers = packet.process_request()
    client_socket.send(headers)
    print(f"Requesting file from server: {file_name}")

    resp = wait_for_response(client_socket)
    file_size = resp['header']['file_size']
    print(resp)
    print(file_size)
    bytes_received = 0
    with open(f'downloaded_{file_name}', 'wb') as f:
        while bytes_received < file_size:
            file_data = client_socket.recv(1024)
            if not file_data:
                break
            file_data_json = json.loads(file_data)
            print(file_data_json['payload']['file_data'])
            file_data_bytes = bytes.fromhex(file_data_json['payload']['file_data'])
            f.write(file_data_bytes)
            bytes_received += len(file_data)
    print("File retrieved.")

def wait_for_response(client_socket):
    while True:
        headers = client_socket.recv(1024).decode() 
        if not headers:
            print("No data received. Closing connection.")
            client_socket.close()  
        try:
            print(headers)
            headers_json = json.loads(headers)  
            print(headers_json)
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            break 
        else:
            return headers_json

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
        response = client_socket.recv(1024)
        print(f"Server response: {response}")

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
    # test_multiple_clients()
    start_client("username-password-recovery-code.csv")