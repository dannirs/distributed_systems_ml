import socket
import os
import threading
from message import message
import json
from handlers import write_to_file

# instead of having separate functions like send_file, just use send_message and pass the parameters
def send_file(file_name, client_socket):
    file_name_size = len(file_name)
    file_size = os.path.getsize(file_name)
    packet = message(
        method="SEND_FILE", 
        source_port=1234, 
        destination_port=5678, 
        header_list={"file_name_size": file_name_size, "file_name": file_name, "file_size": file_size}, 
        file=file_name
    )
    headers, payload = packet.process_request()
    client_socket.send(headers)
    for i in range(len(payload)):
        client_socket.send(payload[i])
    print("File sent.")
    response = client_socket.recv(1024)
    print(f"Server response: {response}")

def get_file(file_name, client_socket):
    file_name_size = len(file_name)
    packet = message(
        method="GET_FILE", 
        source_port=1234, 
        destination_port=5678, 
        header_list={"file_name_size": file_name_size, "file_name": file_name}
    )
    headers = packet.process_request()
    client_socket.send(headers)
    print(f"Requesting file from server: {file_name}")

    resp = wait_for_response(client_socket)
    file_size = resp['header']['file_size']
    new_file_name = f'downloaded_{file_name}'
    write_to_file(client_socket, new_file_name, file_size)
    print("File retrieved.")

def wait_for_response(client_socket):
    while True:
        headers = client_socket.recv(1024).decode() 
        if not headers:
            print("No data received. Closing connection.")
            client_socket.close()  
        try:
            headers_json = json.loads(headers)  
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
    # test_multiple_clients()
    start_client("username-password-recovery-code.csv")