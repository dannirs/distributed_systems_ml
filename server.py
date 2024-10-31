import socket
import os
from message import message
import json
import threading
from handlers import write_to_file
from jsonrpc import dispatcher, JSONRPCResponseManager
import time

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

    @dispatcher.add_method
    def send_file(self, file_name=None, payload_type=None, status=None, file_path=None, seq_num=None, payload=None):
        print(file_path)
        print("server in send_file")
        if payload_type == 2:
            file_path = f"server_files/{file_name}"
            with open(file_path, 'wb') as f:
                file_data_bytes = bytes.fromhex(payload)
                f.write(file_data_bytes)
        print(file_path)
        self.server.file_store[file_name] = (payload_type, file_path)
        print(self.server.file_store)
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
    def retrieve_file(self, file_name=None, payload_type=None, status=None):
        print(self.server.file_store)
        file_path = None
        data_type = 0
        payload_type = 0
        if file_name in self.server.file_store:
            payload_type = self.server.file_store[file_name][0]
            file_path = self.server.file_store[file_name][1]
            status = 200
        else: 
            status = 404
            print(f"File '{file_name}' not found.")

        packet = message( 
            method="retrieve_file_resp", 
            source_port=self.server.port, 
            destination_port=self.server.socket,
            header_list={"file_name": file_name, "file_path": file_path, "status": status, "payload_type": payload_type}
        )        
        
        response = packet.process_headers()
        print("header type: ", type(response))
        # self.server.conn.send(response)
        print(response)
        if status == 200: 
            if payload_type == 2:
                payload = packet.process_payload()
                print(payload)
                json_payload = json.dumps(payload)
                print("payload type: ", type(payload))
            else: 
                payload = {"payload": file_path}
                print("payload type: ", type(payload))
                # json_payload = json.dumps(payload)
                # print("json payload type: ", type(json_payload))
            response.update(payload)
            print("response type: ", type(response))
            # response = json.dumps(response)
        print(type(response))
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
        self.file_store = {}
        self.host = host
        self.port = port
        self.socket = None
        self.conn = None

    def handle_client(self, conn):
        # server_instance = server()

        # Create an instance of the class
        file_service = FileService(self)

        # Register the method using the instance method
        dispatcher["send_file"] = file_service.send_file
        dispatcher["retrieve_file"] = file_service.retrieve_file
        
        # Receive the request from the client
        request = conn.recv(1024).decode('utf-8')
        if not request:
            return False
        print("server handle_client() got request")
        print(request)
        print(type(request))
        marker = "{\"jsonrpc\":"
        # if json_params['params']['payload_type'] == 2:

            # Find the start of the packet
        marker_index = request.index(marker)
        # Check for the next occurrence of the marker to find the end of the packet
        next_marker_index = request.find(marker, marker_index + len(marker))
        if next_marker_index == -1:
            # If there is no second marker, wait for more data
            json_params = json.loads(request)
            if json_params['params']['payload_type'] == 2:
                payload = conn.recv(1024).decode('utf-8')
        # print(payload)
                json_payload = json.loads(payload)
                print(type(json_params['params']))
                print(type(json_payload['params']))
                print(json_params['params'])
                print(json_payload['params'])
                json_params['params'].update(json_payload['params'])
        else: 
            params = request[marker_index:next_marker_index]
            json_params = json.loads(params)
            print(json_params)
            payload = request[next_marker_index:]
            print(payload)
            json_payload = json.loads(payload)
            json_params['params'].update(json_payload['params'])
                

        # json_params = json.loads(request)
        # # print(json_params)
        # if json_params['params']['payload_type'] == 2:
        #     payload = conn.recv(1024).decode('utf-8')
        #     # print(payload)
        #     json_payload = json.loads(payload)
        #     print(type(json_params['params']))
        #     print(type(json_payload['params']))
        #     print(json_params['params'])
        #     print(json_payload['params'])
        #     json_params['params'].update(json_payload['params'])
        request = json.dumps(json_params)
        print(request)

        # Handle the JSON-RPC request and generate a response
        response = JSONRPCResponseManager.handle(request, dispatcher)
        # Send the JSON-RPC response back to the client
        conn.sendall(response.json.encode('utf-8'))
        print("server generated response")
        time.sleep(0.1)
        return False

    def start_server(self, conn, host='localhost', port=5678):
        # conn, addr = server_socket.accept()
        # self.conn = conn
        # self.socket = addr
        # print(f"Connected by {addr}")
        try:
            while True:
                try:
                    # Assuming `handle_client` processes each request in the connection
                    if not self.handle_client(conn):
                        print("Client has finished sending requests.")
                        break
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    # Handle the client disconnecting unexpectedly
                    print("Client disconnected.")
                    break
        finally:
            conn.close()
            print("Connection closed")
    

if __name__ == '__main__':
    rpc_server = server(host='localhost', port=5678)
    host='localhost'
    port=5678
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"TCP JSON-RPC server listening on {host}:{port}")
    while True:  # Loop to keep server open for multiple connections
        conn, addr = server_socket.accept()
        rpc_server.conn = conn
        rpc_server.socket = addr
        print(f"Connected by {addr}")
        print(rpc_server.file_store)
        # Start a new thread to handle each client connection
        client_thread = threading.Thread(target=rpc_server.handle_client, args=(conn,))
        client_thread.start()
        print(rpc_server.file_store)
    # rpc_server.start_server(server_socket)




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
