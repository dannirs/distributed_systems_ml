import socket
import os
import threading
from message import message
import json
from handlers import write_to_file
import random

# set up rpc 
# add port addresses
# test in docker
# fix concurrent connections
# add 2 more methods
# add error handling
# add response messages
# create a manager to manage the whole process?

# client always calls the rpc and sends the method
# send file --> client sends header, payload, etc. 
# get file --> client sends file name
# client always waits for a response
# send file --> an ack is received and client does nothing
# get file --> a flag tells the client to write the file to storage
# send value --> 
# get value --> 
# packet.process_request is called in the rpc
# new headers:
# ack 
# store (only used when server replies to client?)
# type (file vs value)

# on server side, the server receives the send file and it can see from the rpc call which actions to take
# it stores the file and sends an ack
# get file --> retrieves the file and sends it back
# send value --> store the value and sends an ack
# get value --> retrieves and value and sends it back

# instead of having separate functions like send_file, just use send_message and pass the parameters


# def send_file(file_name, client_socket):
#     file_name_size = len(file_name)
#     file_size = os.path.getsize(file_name)
#     packet = message(
#         method="SEND_FILE", 
#         source_port=1234, 
#         destination_port=5678, 
#         header_list={"file_name_size": file_name_size, "file_name": file_name, "file_size": file_size}, 
#         file=file_name
#     )
#     headers, payload = packet.process_request()
#     client_socket.send(headers)
#     for i in range(len(payload)):
#         client_socket.send(payload[i])
#     print("File sent.")
#     response = client_socket.recv(1024)
#     print(f"Server response: {response}")

# def get_file(file_name, client_socket):
#     file_name_size = len(file_name)
#     packet = message(
#         method="GET_FILE", 
#         source_port=1234, 
#         destination_port=5678, 
#         header_list={"file_name_size": file_name_size, "file_name": file_name}
#     )
#     headers = packet.process_request()
#     client_socket.send(headers)
#     print(f"Requesting file from server: {file_name}")

#     resp = wait_for_response(client_socket)
#     file_size = resp['header']['file_size']
#     new_file_name = f'downloaded_{file_name}'
#     write_to_file(client_socket, new_file_name, file_size)
#     print("File retrieved.")

# def wait_for_response(client_socket):
#     while True:
#         headers = client_socket.recv(1024).decode() 
#         if not headers:
#             print("No data received. Closing connection.")
#             client_socket.close()  
#         try:
#             headers_json = json.loads(headers)  
#         except json.JSONDecodeError as e:
#             print(f"Failed to decode JSON: {e}")
#             break 
#         else:
#             return headers_json

class client:
    def __init__(self):
        pass

    def send_message(self, s, request_params):
        jsonrpc = "2.0"
        method = request_params["method"]
        params = request_params["header_list"]
        id = random.randint(1, 40000)
        payload = ""
        if "payload" in request_params:
            payload = request_params["payload"]

        msg = message(
                        method=method, 
                        source_port=s.getsockname(), 
                        destination_port=s.getpeername(),
                        header_list=params,
                        payload=payload
                    )
        headers = message.process_headers(msg)

        request =   {
                        "jsonrpc": jsonrpc,
                        "method": method,
                        "params": headers,
                        "id": id
                    }
        packet = json.dumps(request)
        s.sendall(packet.encode('utf-8'))

        if method == "send_file":
            payload_json = message.process_payload(msg)
            for i in range(len(payload_json)):
                request =   {
                                "jsonrpc": jsonrpc,
                                "method": method,
                                "params": payload_json,
                                "id": id
                            }
                packet = json.dumps(request)
                s.sendall(packet.encode('utf-8'))

        return

    def check_response(self, s, response_data):
        print(response_data)
        if response_data["result"]["status"] != 200: 
            print("Request failed.")
            return False
        elif response_data["result"]["need_to_write"] == True:
            file_name = response_data["result"]['file_name']
            file_size = response_data["result"]['file_size']
            new_file_name = f'downloaded_{file_name}'
            write_to_file(s, new_file_name, file_size)
            print("File retrieved.")
            return True
        elif response_data["result"]["payload_type"] == 1:
            print("Request success.")
            print(response_data["payload"])
        else:
            print("Request success")


    def start_client(self, request_params):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(('localhost', 5678))
            self.send_message(s, request_params)

            response = s.recv(1024).decode('utf-8')
            response_data = json.loads(response)
            self.check_response(s, response_data)
        return

        # process the request_params so it's in a format accepted by the rpc
        # start the socket via RPC call 
        # some different processing depending on the method
        # get the response 
        # parse the header and see if additional action needed
        # close connection 




        # client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # server_address = ('localhost', 65432)  
        # print(f"Connecting to {server_address[0]}:{server_address[1]}")
        # client_socket.connect(server_address)
        
        # try:
        #     send_file(file_name, client_socket)
        #     get_file(file_name, client_socket)

        # finally:
        #     print("Closing connection")
        #     client_socket.close()

    # def test_multiple_clients():
    #     thread1 = threading.Thread(target=start_client, args=("username.csv",))
    #     thread2 = threading.Thread(target=start_client, args=("username-password-recovery-code.csv",))
    #     thread3 = threading.Thread(target=start_client, args=("username-password-recovery-code.csv",))

    #     thread1.start()
    #     thread2.start()
    #     thread3.start()

    #     thread1.join() 
    #     thread2.join() 
    #     thread3.join()

if __name__ == "__main__":
    # test_multiple_clients()
    client = client()
    with open('test_input.json', 'r') as file:
        data = json.load(file)
    client.start_client(data["test_send_file"])
    client.start_client(data["test_retrieve_file"])
    # client.start_client(data["test_send_value"])
    # client.start_client(data["test_receive_value"])
