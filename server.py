import socket
import os
from message import message
import json
import threading

file_store = {}  

def handle_file_upload(client_socket, source_port, destination_port, headers_json):
    file_name_size = headers_json["header"]["file_name_size"]
    file_name = headers_json["header"]["file_name_bytes"]
    file_size = headers_json["header"]["file_size"]
    file_path = f"server_files/{file_name}"

    bytes_received = 0
    with open(file_path, 'wb') as f:
        while bytes_received < file_size:
            file_data = client_socket.recv(1024)
            if not file_data:
                break
            file_data_json = json.loads(file_data)
            print(file_data_json['payload']['file_data'])
            file_data_bytes = bytes.fromhex(file_data_json['payload']['file_data'])
            f.write(file_data_bytes)
            bytes_received += len(file_data_bytes)

    file_store[file_name] = file_path
    print(f"File '{file_name}' received and stored.")
    packet = message(
        method="STORE_FILE", 
        source_port=1234, 
        destination_port=5678, 
        header_list={"status_message": "SUCCESS"}
    )
    headers = packet.process_request()
    client_socket.send(headers)

def handle_file_retrieval(client_socket, source_port, destination_port, headers_json):
    file_name_size = headers_json["header"]["file_name_size"]
    file_name = headers_json["header"]["file_name_bytes"]

    if file_name in file_store:
        file_path = file_store[file_name]
        file_size = os.path.getsize(file_path)
        packet = message(
            method="RETRIEVE_FILE", 
            source_port=1234, 
            destination_port=5678, 
            header_list={"file_size": file_size, "status_message": "SUCCESS"}, 
            file=file_path
        )        
        headers, payload = packet.process_request()
        print(headers)
        client_socket.send(headers)
        for i in range(len(payload)):
            print(payload[i])
            client_socket.send(payload[i])
        print(f"File '{file_name}' sent to client.")
        
    else:
        print(f"File '{file_name}' not found.")
        packet = message(
            method="RETRIEVE_FILE", 
            source_port=1234, 
            destination_port=5678, 
            header_list={"status_message": "FAILURE"}
        )        
        headers = packet.process_request()
        client_socket.send(headers)

def handle_client(client_socket, source_port, destination_port):
    try:
        while True:
            headers = client_socket.recv(1024).decode() 
            if not headers:
                print("No data received. Closing connection.")
                break  
            try:
                headers_json = json.loads(headers)  
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")
                break 

            method = headers_json['header']['method']
            if method == 'SEND_FILE':
                handle_file_upload(client_socket, source_port, destination_port, headers_json)
            elif method == 'GET_FILE':
                handle_file_retrieval(client_socket, source_port, destination_port, headers_json)
            else:
                print(f"Unsupported method: {method}")
                client_socket.close()
    finally:
        client_socket.close()

def start_server(host, port, source_port, destination_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Server listening on {host}:{port}")

    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Connection from {client_address}")
        handle_client(client_socket, source_port, destination_port)

def main():
    host = 'localhost'
    port = 65432
    source_port = 1234
    destination_port = 5678

    start_server(host, port, source_port, destination_port)

if __name__ == "__main__":
    main()
