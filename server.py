import socket
import os
from message import message
import json
import threading
from handlers import write_to_file
from jsonrpc import dispatcher, JSONRPCResponseManager

# send a python file between client and server, and then send the data file for the python program over
# use the rpc feature to call the python program to run and process that data file
# client request: run_program(), arguments specifying the files to use
# save the output in a separate file and return output to the client 
# rpc --> program name, arguments, payload (adding something in the payload or just the headers to call the rpc)
# can create a separate directory for generated program results
# client request: should also add a way to send simple data, like a string or integer, and this shouldn't be stored in a file --> data 
# should have a name and a value
# it can be stored in memory/a cache on the server-side (ex. a dictionary)
# create a manager to manage the whole process 

class FileService:
    def __init__(self, server):
        self.server = server
        self.file_store = {}

    @dispatcher.add_method
    def send_file(self, file_name=None, file_size=None, payload_type=None, status=None, seq_num=None, payload=None):
        print("server in send_file")
        file_path = f"server_files/{file_name}"
        with open(file_path, 'wb') as f:
            file_data_bytes = bytes.fromhex(payload)
            f.write(file_data_bytes)

        self.file_store[file_name] = file_path
        print(self.file_store)
        print(f"File '{file_name}' received and stored.")
        status = 200

        packet = message(
            method="send_file_resp", 
            source_port=self.server.port, 
            destination_port=self.server.socket, 
            header_list={"status": status, "file_name": file_name}
        )
        headers = packet.process_headers()
        return headers 

    @dispatcher.add_method
    def retrieve_file(self, file_name=None, file_size=None, payload_type=None, status=None):
        print(self.file_store)
        file_path = None
        file_size = None
        if file_name in self.file_store:
            file_path = self.file_store[file_name]
            file_size = os.path.getsize(file_path)
            status = 200
        else: 
            status = 404
            print(f"File '{file_name}' not found.")

        packet = message( 
            method="retrieve_file_resp", 
            source_port=self.server.port, 
            destination_port=self.server.socket,
            header_list={"file_name": file_name, "file_path": file_path, "file_size": file_size, "status": status}
        )        
        
        response = packet.process_headers()
        # self.server.conn.send(response)

        if status == 200: 
            payload = packet.process_payload()
            json_payload = json.loads(payload)
            response['params'].update(json_payload['params'])
            response = json.dumps(response)
        
        return response


        # if json_params['params']['payload_type'] == 2:
        #     payload = conn.recv(1024).decode('utf-8')
        #     json_payload = json.loads(payload)
        #     json_params['params'].update(json_payload['params'])
        # request = json.dumps(json_params)
        # print(request)

        #     for i in range(len(payload)):
        #         self.server.socket.send(payload[i])
        #     print(f"File '{file_name}' sent to client.")

class server:
    def __init__(self, host='localport', port=6789):
        self.single_value_store = {}  
        self.host = host
        self.port = port
        self.socket = None
        self.conn = None

    def handle_client(self, conn):
        server_instance = server()

        # Create an instance of the class
        file_service = FileService(server_instance)

        # Register the method using the instance method
        dispatcher["send_file"] = file_service.send_file
        dispatcher["retrieve_file"] = file_service.retrieve_file
        
        # Receive the request from the client
        request = conn.recv(1024).decode('utf-8')
        print("server handle_client() got request")
        json_params = json.loads(request)
        
        if json_params['params']['payload_type'] == 2:
            payload = conn.recv(1024).decode('utf-8')
            print(payload)
            json_payload = json.loads(payload)
            json_params['params'].update(json_payload['params'])
        request = json.dumps(json_params)
        print(request)

        # Handle the JSON-RPC request and generate a response
        response = JSONRPCResponseManager.handle(request, dispatcher)
        print("server generated response")
        # Send the JSON-RPC response back to the client
        conn.sendall(response.json.encode('utf-8'))

    def start_server(self, host='localhost', port=5678):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((host, port))
        server_socket.listen(1)
        print(f"TCP JSON-RPC server listening on {host}:{port}")

        while True:
            conn, addr = server_socket.accept()
            self.conn = conn
            self.socket = addr
            print(f"Connected by {addr}")
            self.handle_client(conn)
            conn.close()
        

if __name__ == '__main__':
    rpc_server = server(host='localhost', port=5678)
    rpc_server.start_server()




    # def handle_file_upload(client_socket, source_port, destination_port, headers_json):
    #     file_name = headers_json["header"]["file_name"]
    #     file_size = headers_json["header"]["file_size"]
    #     file_path = f"server_files/{file_name}"
    #     write_to_file(client_socket, file_path, file_size)

    #     file_store[file_name] = file_path
    #     print(f"File '{file_name}' received and stored.")
    #     packet = message(
    #         method="STORE_FILE", 
    #         source_port=1234, 
    #         destination_port=5678, 
    #         header_list={"status_message": "SUCCESS"}
    #     )
    #     headers = packet.process_request()
    #     client_socket.send(headers)

    # def handle_file_retrieval(client_socket, source_port, destination_port, headers_json):
    #     file_name = headers_json["header"]["file_name"]

    #     if file_name in file_store:
    #         file_path = file_store[file_name]
    #         file_size = os.path.getsize(file_path)
    #         packet = message(
    #             method="RETRIEVE_FILE", 
    #             source_port=1234, 
    #             destination_port=5678, 
    #             header_list={"file_size": file_size, "status_message": "SUCCESS"}, 
    #             file=file_path
    #         )        
    #         headers, payload = packet.process_request()
    #         client_socket.send(headers)
    #         for i in range(len(payload)):
    #             client_socket.send(payload[i])
    #         print(f"File '{file_name}' sent to client.")
            
    #     else:
    #         print(f"File '{file_name}' not found.")
    #         packet = message(
    #             method="RETRIEVE_FILE", 
    #             source_port=1234, 
    #             destination_port=5678, 
    #             header_list={"status_message": "FAILURE"}
    #         )        
    #         headers = packet.process_request()
    #         client_socket.send(headers)

    # def handle_client(client_socket, source_port, destination_port):
    #     try:
    #         while True:
    #             headers = client_socket.recv(1024).decode() 
    #             print(headers)
    #             if not headers:
    #                 print("No data received. Closing connection.")
    #                 break  
    #             try:
    #                 headers_json = json.loads(headers)  
    #             except json.JSONDecodeError as e:
    #                 print(f"Failed to decode JSON: {e}")
    #                 break 

    #             method = headers_json['header']['method']
    #             if method == 'SEND_FILE':
    #                 print("send file")
    #                 handle_file_upload(client_socket, source_port, destination_port, headers_json)
    #             elif method == 'GET_FILE':
    #                 handle_file_retrieval(client_socket, source_port, destination_port, headers_json)
    #             else:
    #                 print(f"Unsupported method: {method}")
    #                 client_socket.close()
    #     finally:
    #         client_socket.close()

    # def start_server(host, port, source_port, destination_port):
    #     server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #     server_socket.bind((host, port))
    #     server_socket.listen(5)
    #     print(f"Server listening on {host}:{port}")

    #     while True:
    #         client_socket, client_address = server_socket.accept()
    #         print(f"Connection from {client_address}")
    #         handle_client(client_socket, source_port, destination_port)

    # def main():
    #     host = 'localhost'
    #     port = 65432
    #     source_port = 1234
    #     destination_port = 5678

    #     start_server(host, port, source_port, destination_port)

    # if __name__ == "__main__":
    #     main()
