import socket
from message import message
import json
import threading
from handlers import write_to_file 
from jsonrpc import dispatcher, JSONRPCResponseManager
import time
import random

# write a script so that instead of the workers being started by MasterNode, use the script to start
# the workers when the configs are located in different directories. Configuration file
# names should be the same.
# dir1 - node1
# dir2 - node2
# dir5 - program files
# script, run with bash so I don't have to type the command line arguments every time
# MapReduce:
# create an interface to the map phase, reduce phase
# rpc call starting from userclient, sending instruction to masternode, masternode
# decomposes instruction to subtask instructions and distributes chunks to worker nodes
# worker node then runs the map call on the data
# at the end, the results of the map should be saved to a file, locally on the worker node
# one worker node probably contains several data chunks; each chunk produces 1 result
# the result is stored on the worker node's local storage
# data_registry - location of the files/keys itself 
# mapreduce_registry - location of the mapreduce computation
# mapreduce_datastore - stores computation itself
# taskID: {orig_file1: [chunk1comp, chunk2comp], orig_file2: [chunk1comp, chunk2comp]}
# reduce phase:
# do all of the reduce computation on only 1 node - simple implementation 
# JobManager picks 1 worker as the worker responsible for the reduce tasks
# JobManager sends instructions to all worker nodes to have them send their map computations
# to the target node 
# JobManager can send an instruction to each worker node to have them send their map
# computation directly to the target node, or MasterNode sends an instruction to the
# target node containing information on location of each map computation, and the target
# node uses the information to get files from the worker node. This can be done in parallel
# or in sequence
# depends which performance is better
# test using data from hadoop, etc. 
# tree: 2    tree: 3
# tree: 5
# only 1 target node, so we skip shuffle 
# after the reduce task, the result is stored locally on the target node
# the location of the result is sent to MasterNode's DataManager
# for now implement 1 phase mapreduce, can also implement multiple phase mapreduce later
# k-means is multiple mapreduce
# test mapreduce procedure using multiple nodes, use simple word count example and other examples
# consider fault tolerance; client is not available during map --> rerun task 
# server not available and data is lost --> return all tasks on that node
# can create a replica of each data file/computation chunk and store it on 3 worker nodes (optional implementation, don't have to do it right now)

# later on, linear regression can be done in just 1 phase
# ML program 
# Test with the actual data
# Deploy on cloud (can test it out if I have time) --> simple way is to need 4 images, 1 for each node, just run each container image
# a better way is to create 1 image, and run 4 containers, but each container is different (needs to be configured)

class FileService:
    def __init__(self, server):
        self.server = server

    @dispatcher.add_method
    def send_data(self, key=None, payload_type=None, method=None, source_port=None, destination_port=None, status=None, file_path=None, seq_num=None, payload=None):
        print("Server is in send_data().")
        if payload_type == 2:
            file_path = f"server_files/{key}"
            write_to_file(payload, file_path)

        self.server.file_store[key] = (payload_type, file_path)
        print(f"'{key}' received and stored in cache.")
        status = 200
        self.send_data_location(key)

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
        if key in self.server.file_store:
            payload_type = self.server.file_store[key][0]
            file_path = self.server.file_store[key][1]
            status = 200
        else:
            status = 404
            print(f"'{key}' not found in cache.") 

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

        if status == 200 and payload_type == 2:
            payload = packet.process_payload()
            response.update(payload)
        return response


    @dispatcher.add_method
    def send_data_location(self, key=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("Server is sending data location")
            s.connect((self.server.master_ip, self.server.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "data.store_data_location",
                "params": {
                    "original_file_name": key,
                    "client_address": (self.server.ip, self.server.port)
                },
                "id": 1
            }
            s.sendall(json.dumps(request).encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            print("Data location sent:", response)

    @dispatcher.add_method
    def send_task_to_client(self, client_address=None, task_data=None):
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
        try:
            method = ""
            file_service = FileService(self)
            dispatcher["send_data"] = file_service.send_data
            dispatcher["retrieve_data"] = file_service.retrieve_data
            dispatcher["send_data_location"] = file_service.send_data_location
            dispatcher["send_task_to_client"] = file_service.send_task_to_client

            request = conn.recv(1024).decode('utf-8')
            print("From client: ", request)
            if not request:
                return False
            print("Server received client's request in handle_client().")
            try:
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
            
            elif isinstance(request, str):
                try:
                    parsed_request = json.loads(request)
                    print("DEBUG: Request is a valid JSON string.")
                except json.JSONDecodeError as e:
                    print(f"ERROR: Invalid JSON string: {e}")
                    return {"error": {"code": -32600, "message": "Invalid Request"}, "id": None, "jsonrpc": "2.0"}

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

if __name__ == '__main__':
    rpc_server = WorkerServer()
