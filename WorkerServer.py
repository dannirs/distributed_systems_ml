import socket
from message import message
import json
import threading
from handlers import write_to_file
from jsonrpc import dispatcher, JSONRPCResponseManager
import time

class FileService:
    def __init__(self, server):
        self.server = server

    @dispatcher.add_method
    def send_data(self, key=None, payload_type=None, method=None, source_port=None, destination_port=None, status=None, file_path=None, seq_num=None, payload=None):
        print("Server is in send_data().")

        # If the data sent is a file, a new file is created in the server's local directory and the payload
        # is written to the file. 
        if payload_type == 2:
            file_path = f"server_files/{key}"
            write_to_file(payload, file_path)

        # The key-value pair is written to the server's local cache, where the key is either the name of the file
        # or the name assigned to the single value.
        # The value is a tuple that includes payload_type, which indicates whether the value is a single value
        # or the path to a file.
        self.server.file_store[key] = (payload_type, file_path)
        print(f"'{key}' received and stored in cache.")
        status = 200

        # A response is returned to the client using the method send_data_resp. The response includes the status 
        # to indicate if the request succeeded, and the key.
        # To package the response, process_headers() is called. For this specific response, the function is mainly for
        # the purpose of error handling (checking that all required fields are included and as expected).
        packet = message( 
            method="send_data_resp", 
            source_port=destination_port,
            destination_port=source_port, 
            header_list={"status": status, "key": key}
        )
        headers = packet.process_headers()
        return headers 

    @dispatcher.add_method
    def retrieve_data(self, key=None, payload_type=None, method=None, source_port=None, destination_port=None, status=None):
        # When the server receives a request to retrieve previously stored data, it checks its local cache to
        # see if the given key can be found. 
        # If found, the server can retrieve the data type and the value (either a file path or a single value).
        if key in self.server.file_store:
            payload_type = self.server.file_store[key][0]
            file_path = self.server.file_store[key][1]
            status = 200
        else: 
            status = 404
            print(f"'{key}' not found in cache.")

        # A response is returned to the client using the method retrieve_data_resp. The status and data type are
        # included so that the client knows how to process the response.
        # For the payload, if the value is a single value, then the value is written directly into the payload.
        payload = ""
        if status == 200 and payload_type == 1: 
            payload = file_path

        packet = message( 
            method="retrieve_data_resp", 
            source_port=destination_port,
            destination_port=source_port,
            header_list={"key": key, "status": status, "payload_type": payload_type, "payload": payload}
        )        
        
        response = packet.process_headers()

        # if the payload is the contents of a file, then process_payload() is called to read from the file 
        # and send the results back to the client. The headers and payload are sent in the same packet by
        # aadding the payload as a header. 
        # TODO: should send payload and response separately
        if status == 200 and payload_type == 2:
            payload = packet.process_payload()
            response.update(payload)
        return response

class WorkerServer:
    def __init__(self):
        self.file_store = {}
        self.
 
    def handle_client(self, conn):
        # The RPC is initiated as an instance, with the server being passed in as a parameter so that the
        # RPC can access the server's local cache. send_data() and retrieve_data are the 2 methods that the RPC
        # can call. 
        file_service = FileService(self)
        dispatcher["send_data"] = file_service.send_data
        dispatcher["retrieve_data"] = file_service.retrieve_data

        # The server reads the request from the client.
        # When the client sends the headers and payload separately, the server may have already received 
        # both packets, or the server may still be waiting for the payload to arrive. This is handled by using the "jsonrpc:" 
        # field as the delimiter between packets. 
        request = conn.recv(1024).decode('utf-8')
        if not request:
            return False
        print("Server received client's request in handle_client().")
        marker = "{\"jsonrpc\":"
        marker_index = request.index(marker)
        next_marker_index = request.find(marker, marker_index + len(marker))

        # For the current implementation, there will only be 2 packets: 1 header packet and 1 payload packet.
        # If there is only 1 packet received and the expected data type is a file, then the server will wait
        # to receive the rest of the payload before adding the payload packet to the header packet.
        # If the expected data type is a single value, there will only be one packet total and the server can
        # immediately proceed.
        # If the data type is a file and both packets were already received, the server will split the packets 
        # using the delimiter before adding the payload data as a header to the header packet.
        # TODO: factor in the fact that there might be multiple packets for the payload
        if next_marker_index == -1:
            json_params = json.loads(request)
            if json_params['params']['payload_type'] == 2:
                payload = conn.recv(1024).decode('utf-8')
                json_payload = json.loads(payload)
                json_params['params'].update(json_payload['params'])
        else: 
            params = request[marker_index:next_marker_index]
            json_params = json.loads(params)
            payload = request[next_marker_index:]
            json_payload = json.loads(payload)
            json_params['params'].update(json_payload['params'])

        request = json.dumps(json_params)

        # The server then calls the RPC to execute the correct method. The RPC will also create
        # a response message to send back to the client, and the server will send this message.
        # In order to ensure that the client receives the response before the connection is closed,
        # a brief wait time is added before the server closes the connection.
        response = JSONRPCResponseManager.handle(request, dispatcher)
        conn.sendall(response.json.encode('utf-8'))
        print("Server generated response.")
        time.sleep(0.1)
        return False

    def start_server(self, conn):
        try:
            while True:
                try:
                    # Once the client has stopped sending requests and the response message has been
                    # sent back, the connection wth the client is terminated.
                    if not self.handle_client(conn):
                        print("Client has finished sending requests.")
                        break
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    print("Client disconnected.")
                    break
        finally:
            conn.close()
            print("Connection closed")
    

if __name__ == '__main__':
    # A server object is initialized, with a default host and port set. 
    # The server listens for 5 threads at a time. The server remains open
    # even after the connection with a client is terminated.
    # When a client thread is connected, the server goes into the handle_client() method.
    rpc_server = WorkerServer()
    host = 'localhost'
    port =  5678
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"TCP JSON-RPC server listening on {host}:{port}")
    while True:  
        conn, addr = server_socket.accept()
        print(f"Connected to {addr}")
        client_thread = threading.Thread(target=rpc_server.start_server, args=(conn,))
        client_thread.start()
