 # handlers.py
import socket
import json
from message import message

def write_to_file(payload, file_name):
    print(type(payload))
    with open(file_name, 'wb') as f:
        file_data_bytes = bytes.fromhex(payload)
        f.write(file_data_bytes)


# def write_to_file(payload, file_name, file_size):
#     bytes_received = 0
#     with open(file_name, 'wb') as f:
#         while bytes_received < file_size:
#             file_data = client_socket.recv(1024)
#             if not file_data:
#                 break
#             file_data_json = json.loads(file_data)
#             print(file_data_json)
#             file_data_bytes = bytes.fromhex(file_data_json['params']['payload'])
#             f.write(file_data_bytes)
#             bytes_received += len(file_data)

