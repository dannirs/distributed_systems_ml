import socket
from message import message
import json
import threading
from handlers import write_to_file 
from jsonrpc import dispatcher, JSONRPCResponseManager
import time
import random

# Node:
# -Splits job into tasks
# -Splits large file into chunks (check through the tasks to see which files need to be split)
# -Get available workers from MasterNode
# -Send split file 
# -User inputs command line arguments (replaces job.json file)
# MasterNode: 
# -Add jsonrpc
# -Add a datamanager which stores where each piece of the same file has been stored
# DataManager:
# -Stores files as (key: [(file_name, addr), (file_name,addr)])
# -Name of file is written as: data.txt ---> data.txt1, data.txt2, data.txt3
# JobManager:
# -Add jsonrpc
# -Send task directly to taskmanager instead of client
# -If no task progress message received, JobManager sends an instruction to kill the task and restarts it on a different client
# -When the task is complete, JobManager removes it from the queue
# -When job is complete, JobManager reports completion to UserClient
# -Tasks run in parallel, no dependencies
# TaskManager:
# -Add jsonrpc
# -Sends heartbeat about the task to JobManager (still in progress message)
# -Reports when the task is complete
# Worker:
# -Is a separate class that contains self.server and self.clients 
# -When the worker node is initialized, the server is connected to the master and listens for requests
# -When the server gets a task, it starts up the client and sends the task
# Client:
# -Calls DataManager in MasterNode to get server location and then connects to correct server

# JobManager sends RPC instruction to taskmanager and taskmanager executes the instruction
# JobManager, MasterNode, etc. should all use jsonrpc
# Add periodic reporting of the task (still in progress message)
# If no heartbeat received, JobManager sends another instruction to kill the task and restart on a
# different client
# TaskManager - reports on task progress, reports when task complete
# MasterNode has a DataClient/Manager --> data can be split into many chunks, it stores where
# each piece of the data is stored (key: [(file_name, addr), (file_name,addr)])
# (ip, port)
# name of file when data is split into chunks:
# data.txt ---> data.txt1, data.txt2, data.txt3
# userclient splits large file into chunks and sends to available workers (this can also be done by 
# jobmanager), gets available workers from 
# masternode; userclient checks through all the tasks to see which files need to be split
# WorkerNode contains a data storage (data source) that holds data locally  
# client task to retrieve data --> call datamanager in masternode to get server location --> 
# connect to the correct server after getting response (server = worker, client = client)
# job = task1, task2, task3 (tasks should run in parallel)
# No interdependencies between tasks in the same job
# map phase = 1 job, reduce phase = another job
# UserClient - replaces the job.json file, the user inputs command line arguments
# change Node class to UserClient
# Worker node --> self.server, self.clients
# worker node is initialized, server is connected to master and listens for requests, when the server
# gets a task, 
# start with 3 worker nodes, each node is in its own directory, 1 masternode
# each worker node contains 1 server, any number of clients (1 or more)


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
        self.send_data_location(key)

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


    @dispatcher.add_method
    def send_data_location(self, key=None):
        # Send data location to the master
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Server is sending data location")
            s.connect((self.server.master_ip, self.server.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "store_data_location",
                "params": {
                    "key": key,
                    "address": (self.server.ip, self.server.port)
                },
                "id": 1
            }
            s.sendall(json.dumps(request).encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            print("Data location sent:", response)

    @dispatcher.add_method
    def send_task_to_client(self, client_address=None, task_data=None):
        """
        Handles task requests from the JobManager.
        """
        print(f"WorkerServer received task: {task_data}")
        jsonrpc = "2.0"
        id = random.randint(1, 40000)
        message =   {
                        "jsonrpc": jsonrpc,
                        "method": task_data["method"],
                        "params": task_data,
                        "id": id
                    }
        print(message)
        client = self.server.worker.get_client((client_address[0], client_address[1]))
        client.handle_task(message)

class WorkerServer:
    def __init__(self, master_ip, master_port, ip, port, worker):
        self.file_store = {}
        self.master_ip = master_ip
        self.master_port = master_port
        self.ip = ip
        self.port = port
        self.worker = worker

    def handle_client(self, conn):
        # The RPC is initiated as an instance, with the server being passed in as a parameter so that the
        # RPC can access the server's local cache. send_data() and retrieve_data are the 2 methods that the RPC
        # can call. 
        try:
            method = ""
            file_service = FileService(self)
            dispatcher["send_data"] = file_service.send_data
            dispatcher["retrieve_data"] = file_service.retrieve_data
            dispatcher["send_data_location"] = file_service.send_data_location
            dispatcher["send_task_to_client"] = file_service.send_task_to_client

            # The server reads the request from the client.
            # When the client sends the headers and payload separately, the server may have already received 
            # both packets, or the server may still be waiting for the payload to arrive. This is handled by using the "jsonrpc:" 
            # field as the delimiter between packets. 
            request = conn.recv(1024).decode('utf-8')
            print("From client: ", request)
            if not request:
                return False
            print("Server received client's request in handle_client().")
            try:
                # Try parsing the entire string as one dictionary
                parsed = json.loads(request)
                print(type(parsed["params"]))
                if isinstance(parsed, dict):
                    method = parsed["method"]
                    print("method1: ", method)
                else: 
                    dicts = [json.loads(part) for part in parsed.split("}{")]
                    method = dicts[0]["method"]
                    print("method2: ", method)
            except json.JSONDecodeError as e:
                print(f"ERROR: Invalid JSON string: {e}")
                pass
            if not method or method != "send_task_to_client":
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
            if isinstance(request, dict):
                print("DEBUG: Request is a dictionary. Serializing to JSON string.")
                request = json.dumps(request)
            
            # If the request is a string, ensure it's valid JSON
            elif isinstance(request, str):
                try:
                    # Attempt to parse the string as JSON to validate it
                    parsed_request = json.loads(request)
                    print("DEBUG: Request is a valid JSON string.")
                except json.JSONDecodeError as e:
                    print(f"ERROR: Invalid JSON string: {e}")
                    return {"error": {"code": -32600, "message": "Invalid Request"}, "id": None, "jsonrpc": "2.0"}

            # The server then calls the RPC to execute the correct method. The RPC will also create
            # a response message to send back to the client, and the server will send this message.
            # In order to ensure that the client receives the response before the connection is closed,
            # a brief wait time is added before the server closes the connection.
            
            # request = json.dumps(request)
            response = JSONRPCResponseManager.handle(request, dispatcher)
            print("response: ", response.json)
            conn.sendall(response.json.encode('utf-8'))
            print("Server generated response.")
            time.sleep(0.1)
        except Exception as e:
            print(f"Exception in handle_client: {e}")
        finally:
            conn.close()
            print("Connection closed")

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"TCP JSON-RPC server listening on {self.ip}:{self.port}")
        while True:  
            print("Waiting for connection...")
            conn, addr = server_socket.accept()
            print(f"In server, Connected to {addr}")
            try:
                client_thread = threading.Thread(target=self.handle_client, args=(conn,))
                client_thread.start()
                print(f"Started thread for connection: {addr}")
            except Exception as e:
                print(f"Error starting thread for {addr}: {e}")
            # try:
            #     while True:
            #         try:
            #             # Once the client has stopped sending requests and the response message has been
            #             # sent back, the connection wth the client is terminated.
            #             if not self.handle_client(conn):
            #                 print("Client has finished sending requests.")
            #                 break
            #         except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
            #             print("Client disconnected.")
            #             break
            # finally:
            #     conn.close()
            #     print("Connection closed")

if __name__ == '__main__':
    # A server object is initialized, with a default host and port set. 
    # The server listens for 5 threads at a time. The server remains open
    # even after the connection with a client is terminated.
    # When a client thread is connected, the server goes into the handle_client() method.
    rpc_server = WorkerServer()
