from MasterNode import MasterNode
import json
import random
import os
import argparse
import csv
import socket
import threading
import json
from JSONRPCProxy import JSONRPCProxy
from jsonrpc import dispatcher, JSONRPCResponseManager
from message import message
import base64
from handlers import log_data_summary

# 1. Set up the script to run configs in different directories
# 2. Finish debugging Reduce phase 
# 3. Test chunking the files
# 4. Implement ML algorithm (linear regression)

# for linear regression, need to produce individual results for each chunk
# test dataset and the actual dataset 
# map phase - chunk1res, chunk2res (keys-value pairs) --> 10 chunks --> execute linear regression, add it to map func.
# get partial results from all individual chunks, and then combine them to get the final model in the reduce phase
# reduce phase - final result (use aggregate approach, aggregates all key-value pairs) --> 1 result
# weights - player salaries, daily fantasy algorithm for scoring, total money available
# can get the average for partial data, but how to get the total average in the aggregate? 
# use intermediate result for partial data
# ex. computing the average for the whole data
# points 2022  minutes 2022
# points 2023  minutes 2023
# reduce - average
# mapreduce task --> check if files chunked --> map_file1_1, map_file1_2, map_file2, reduce_(file1,file2)
# jobmanager --> map tasks executed, all tasks succeed, reduce task executed
# reduce client --> datamanager --> retrieve all map results --> sends map results to reduce server 
# Can load linear regression coefficients etc. in json

# optimal team: [players] 

# script can call python programs to run code like initializing the worker object
# script is to get the different config files from different directories and call start_master,
# start_workers, etc. --> is the start to the program

# start program by calling config file, which starts userclient --> enter list of configuration paths, command line arguments
# for job
# similar to scala, the shell file should be a user interface to start up UserClient 
# as a test program, can have a script that already has all command line arguments and jobs and configs; just returns
# a result

# final report: use the weekly report and make it more formal; highlight key milestones (same day as deliverable)
# Tutorial for the user to use the program
# when delivering, put everything in one package
# can be later (dec. 17, etc.) 

class PacketManager:
    def __init__(self):
        self.packet_store = {}  # Store packets indexed by job/request ID

    def store_packets(self, job_id, packets):
        self.packet_store[job_id] = packets

    def get_next_packet(self, job_id, seq_num):
        if job_id not in self.packet_store:
            raise ValueError(f"Job ID {job_id} not found")

        packets = self.packet_store[job_id]
        if seq_num < len(packets):
            return packets[seq_num]
        else:
            raise ValueError(f"Sequence number {seq_num} out of range for Job ID {job_id}")

# Store packets after processing
def retrieve_data(self, header_list=None):
    payload_type = 2
    key = header_list["key"]
    destination_port = header_list["destination_port"]
    source_port = header_list["source_port"]

    if not key or not os.path.exists(key):
        print(f"File '{key}' not found")
        return {"status": 404, "message": f"'{key}' not found in cache."}

    # Process payload into packets
    packets = self.process_payload()
    job_id = random.randint(1, 40000)  # Generate a unique job ID

    # Store packets for sequential transmission
    self.packet_manager.store_packets(job_id, packets)
    return {"status": 200, "job_id": job_id, "message": "Job initialized, ready for packet requests."}

def send_packet(self, job_id, seq_num):
    try:
        packet = self.packet_manager.get_next_packet(job_id, seq_num)
        return {"status": 200, "packet": packet}
    except ValueError as e:
        return {"status": 400, "message": str(e)}


class FileService:
    def __init__(self, server):
        pass

    @dispatcher.add_method
    def retrieve_data(self, header_list=None):
        payload_type = 2
        key = header_list["key"]
        destination_port = header_list["destination_port"]
        source_port = header_list["source_port"]
        if not key or not os.path.exists(key):
            print(f"File '{key}' not found")
            status = 404
            print(f"'{key}' not found in cache.") 
        else: 
            status = 200

        payload = ""
        if status == 200: 
            payload = key

        packet = message( 
            method="retrieve_data_resp", 
            source_port=destination_port,
            destination_port=source_port,
            header_list={"key": key, "status": status, "payload_type": payload_type, "payload": payload}
        )        
        
        response = packet.process_headers()
        print(response)

        if status == 200:
            payloads = packet.process_payload()
            print(payloads)
            print("length of payload: ", len(payloads))
            for payload in payloads:
                try:
                    decoded_payload = base64.b64decode(payload["payload"]).decode('utf-8')  # Decode base64
                    json_payload = json.loads(decoded_payload)  # Parse JSON
                    print("Decoded JSON payload:", json_payload)
                    # response.update(json_payload)
                except (base64.binascii.Error, json.JSONDecodeError) as e:
                    print(f"Error decoding or parsing payload: {e}")
                    continue
        print("retrieve data response: ", response)
        return response

class Client:
    def __init__(self, master_ip, master_port, job_files, client_ip="localhost", client_port=2001):
        self.master_ip = master_ip
        self.master_port = master_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.running = True
        self.files = []
        self.job_files = job_files
        # self.server_thread = threading.Thread(target=self.start_server, daemon=True)
        # self.server_thread.start()
 

        # self.master = None
        # self.config_files = config_files
        # self.start_master()
    
    def process_job(self):
        for job_file in self.job_files:
            job_file = job_file.strip()
            if os.path.exists(job_file):
                self.handle_job_submission(job_file)
            else:
                print(f"Error: File '{job_file}' does not exist.")
        

    # def start_master(self):
    #     for config_file in self.config_files:
    #         try:
    #             with open(config_file, "r") as file:
    #                 configs = json.load(file)

    #             for node in configs:
    #                 if node.get("is_master") and len(node) == 3:  
    #                     ip = node.get("ip")
    #                     port = node.get("port")
    #                     print("MasterNode IP: ", ip)
    #                     print("MasterNode Port: ", port)
    #                     self.config_files.remove(config_file)
    #                     self.master = MasterNode(self.config_files, ip, port)
    #                     self.master.start()
    #                     return  
    #         except Exception as e:
    #             print(f"Error processing {config_file}: {e}")

    def handle_job_submission(self, job_file):
        print(f"Processing job file: {job_file}")
        try:
            with open(job_file, 'r') as file:
                job_data = json.load(file)
            
            tasks = self.create_tasks(job_data)
            print(f"Job from {job_file} received and tasks created and added to the queue.")
            self.send_job(tasks)
        except Exception as e:
            print(f"Error processing job file {job_file}: {e}")

    def handle_command_line_job(self, tasks):
        """
        Process a job provided directly via command-line arguments.
        """
        tasks = self.create_tasks(tasks)
        print("Job received from command-line arguments and tasks created.")
        self.send_job(tasks)

    def create_tasks(self, job_data):
        tasks = []
        job_id = random.randint(1, 40000)
        max_chunk_size = 2520 * 1024  # Chunk size in bytes (10 KB)

        for dictionary in job_data:
            method = dictionary.get("method")
            payload = dictionary.get("payload")
            header_list = dictionary.get("header_list", {})
            file_path = header_list.get("key")
            file_paths = header_list.get("keys", [])  # For mapreduce tasks

            # Handle send_data
            if method == "send_data":
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": {"key": file_path},
                    "payload": payload
                }
                tasks.append(task)
                self.files.append(file_path)

            # Handle retrieve_data
            elif method == "retrieve_data":
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": header_list,
                    "payload": payload
                }
                tasks.append(task)

            # Handle mapreduce
            elif method == "mapreduce" :
                if not all(os.path.exists(fp) for fp in file_paths):
                    print(f"One or more files in file_paths are invalid: {file_paths}")
                    continue

                map_results = []  # To store map result paths for reduce

                for file_path in file_paths:
                    file_size = os.path.getsize(file_path)
                    print("file size: ", file_size)
                    print("max chunk size: ", max_chunk_size)

                    if file_size > max_chunk_size:  # Chunk large CSV files
                        print(f"Chunking large file for mapreduce: {file_path}")
                        base_name, ext = os.path.splitext(file_path)
                        chunk_id = 0



                        # with open(file_path, "rb") as file:
                        #     data = file.read()
                        #     result = chardet.detect(data)
                        #     print(result['encoding'])

                        with open(file_path, 'r', newline='', encoding="utf-8") as csv_file:
                            reader = csv.reader(csv_file)
                            header = next(reader)  # Extract the header row

                            chunk_rows = []
                            chunk_size = 0

                            for row in reader:
                                row_size = sum(len(str(cell)) for cell in row)
                                if chunk_size + row_size > max_chunk_size:
                                    # Write current chunk to a file
                                    chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}"
                                    with open(chunk_file_name, 'w', newline='', encoding='utf-8') as chunk_file:
                                        writer = csv.writer(chunk_file)
                                        writer.writerow(header)  # Write the header
                                        writer.writerows(chunk_rows)  # Write the rows

                                    # Create a Map task for the chunk
                                    tasks.append({
                                        "job_id": job_id,
                                        "task_id": random.randint(1, 40000),
                                        "method": "map",
                                        "header_list": {"original_file": file_path, "key": chunk_file_name},
                                        "payload": None
                                    })
                                    map_results.append(chunk_file_name)  # Add chunk name to map results
                                    self.files.append(chunk_file_name)
                                    # Reset chunk data
                                    chunk_rows = []
                                    chunk_size = 0
                                    chunk_id += 1

                                # Add the current row to the chunk
                                chunk_rows.append(row)
                                chunk_size += row_size

                            # Write the last chunk if it exists
                            if chunk_rows:
                                chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}"
                                with open(chunk_file_name, 'w', newline='', encoding='utf-8') as chunk_file:
                                    writer = csv.writer(chunk_file)
                                    writer.writerow(header)  # Write the header
                                    writer.writerows(chunk_rows)  # Write the rows

                                # Create a Map task for the chunk
                                tasks.append({
                                    "job_id": job_id,
                                    "task_id": random.randint(1, 40000),
                                    "method": "map",
                                    "header_list": {"original_file": file_path, "key": chunk_file_name},
                                    "payload": None
                                })
                                map_results.append(chunk_file_name)  # Add chunk name to map results
                                self.files.append(chunk_file_name)
                    else:
                        # Single Map task for small files
                        tasks.append({
                            "job_id": job_id,
                            "task_id": random.randint(1, 40000),
                            "method": "map",
                            "header_list": {"key": file_path},
                            "payload": None
                        })
                        map_results.append(file_path)  # Add original file name to map results
                        self.files.append(file_path)
                # Create Reduce task with populated map results
                reduce_task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": "reduce",
                    "header_list": {"map_results": map_results},  # Populate with map result paths
                    "payload": None
                }
                tasks.append(reduce_task)
                # for res in map_results:
                #     self.files.append(res)
            else:
                print(f"Unsupported method: {method}")
        for file in self.files:
            self.send_data_location(file, (self.client_ip, self.client_port))
        return tasks

    def send_file_chunks(self, data):
        """Send file chunks to the assigned worker."""
        print("in send_file_chunks")
        worker_ip = data["worker_ip"]
        worker_port = data["worker_port"]
        file_chunks = data["file_chunks"]

        for chunk in file_chunks:
            chunk_path = f"./chunks/{chunk}"  # Update with actual file path
            try:
                with open(chunk_path, "rb") as f:
                    file_data = f.read()
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((worker_ip, worker_port))
                    s.sendall(file_data)
                    print(f"Sent chunk {chunk} to Worker at {worker_ip}:{worker_port}")
            except Exception as e:
                print(f"Failed to send chunk {chunk} to Worker: {e}")


    # def create_tasks(self, job_data):
    #     tasks = []
    #     job_id = random.randint(1, 40000)
    #     max_chunk_size = 10 * 1024 * 1024  # Chunk size in bytes (10 MB)

    #     for dictionary in job_data:
    #         method = dictionary.get("method")
    #         payload = dictionary.get("payload")
    #         header_list = dictionary.get("header_list", {})
    #         file_path = header_list.get("key")
    #         file_paths = header_list.get("keys", [])  # For mapreduce tasks

    #         # Handle send_data
    #         if method == "send_data":
    #             if file_path and not os.path.exists(file_path):
    #                 print(f"Invalid or missing file path: {file_path}")
    #                 continue

    #             # Single task for sending the file
    #             task = {
    #                 "job_id": job_id,
    #                 "task_id": random.randint(1, 40000),
    #                 "method": method,
    #                 "header_list": {"file_path": file_path},
    #                 "payload": payload
    #             }
    #             tasks.append(task)

    #         # Handle retrieve_data
    #         elif method == "retrieve_data":
    #             # Single task for retrieving the file
    #             task = {
    #                 "job_id": job_id,
    #                 "task_id": random.randint(1, 40000),
    #                 "method": method,
    #                 "header_list": header_list,
    #                 "payload": payload
    #             }
    #             tasks.append(task)

    #         # Handle mapreduce
    #         elif method == "mapreduce":
    #             if not all(os.path.exists(fp) for fp in file_paths):
    #                 print(f"One or more files in file_paths are invalid: {file_paths}")
    #                 continue

    #             # Generate Map tasks
    #             map_tasks = []
    #             reduce_task = {
    #                 "job_id": job_id,
    #                 "task_id": random.randint(1, 40000),
    #                 "method": "reduce",
    #                 "header_list": {"map_results": []},  # To be populated later
    #                 "payload": None
    #             }

    #             for file_path in file_paths:
    #                 file_size = os.path.getsize(file_path)

    #                 if file_size > max_chunk_size:  # Chunk large files
    #                     print(f"Chunking large file for mapreduce: {file_path}")
    #                     base_name, ext = os.path.splitext(file_path)
    #                     num_chunks = -(-file_size // max_chunk_size)

    #                     with open(file_path, 'rb') as f:
    #                         for chunk_id in range(num_chunks):
    #                             chunk_data = f.read(max_chunk_size)
    #                             chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}"

    #                             # Save the chunk locally
    #                             with open(chunk_file_name, 'wb') as chunk_file:
    #                                 chunk_file.write(chunk_data)

    #                             # Create a Map task for each chunk
    #                             map_tasks.append({
    #                                 "job_id": job_id,
    #                                 "task_id": random.randint(1, 40000),
    #                                 "method": "map",
    #                                 "header_list": {"original_file": file_path, "key": chunk_file_name},
    #                                 "payload": None
    #                             })
    #                 else:
    #                     # Single Map task for small files
    #                     map_tasks.append({
    #                         "job_id": job_id,
    #                         "task_id": random.randint(1, 40000),
    #                         "method": "map",
    #                         "header_list": {"key": file_path},
    #                         "payload": None
    #                     })

    #             # Add Map tasks and the Reduce task
    #             tasks.extend(map_tasks)
    #             tasks.append(reduce_task)

    #         else:
    #             print(f"Unsupported method: {method}")

    #     return tasks



    # def create_tasks(self, job_data):
    #     tasks = []
    #     job_id = random.randint(1, 40000)
    #     max_chunk_size = 10 * 1024 * 1024  

    #     for dictionary in job_data:
    #         method = dictionary.get("method")
    #         payload = dictionary.get("payload")
    #         header_list = dictionary.get("header_list", {})

    #         if method == "send_file" and not payload:  
    #             file_path = header_list.get("file_path")
    #             if not os.path.exists(file_path):
    #                 print(f"File not found: {file_path}")
    #                 continue

    #             file_size = os.path.getsize(file_path)
    #             if file_size > max_chunk_size:
    #                 print(f"File too large, splitting: {file_path}")
    #                 base_name, ext = os.path.splitext(file_path)  
    #                 num_chunks = -(-file_size // max_chunk_size)  

    #                 with open(file_path, 'rb') as f:
    #                     for chunk_id in range(num_chunks):
    #                         chunk_data = f.read(max_chunk_size)
    #                         chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}" 
    #                         with open(chunk_file_name, 'wb') as chunk_file:
    #                             chunk_file.write(chunk_data)

    #                         task = {
    #                             "job_id": job_id,
    #                             "task_id": random.randint(1, 40000),
    #                             "method": method,
    #                             "header_list": {"original_file_name": file_path, "file_path": chunk_file_name},
    #                             "payload": None
    #                         }
    #                         tasks.append(task)
    #             else:
    #                 task = {
    #                     "job_id": job_id,
    #                     "task_id": random.randint(1, 40000),
    #                     "method": method,
    #                     "header_list": header_list,
    #                     "payload": payload
    #                 }
    #                 tasks.append(task)
    #         else:
    #             task = {
    #                 "job_id": job_id,
    #                 "task_id": random.randint(1, 40000),
    #                 "method": method,
    #                 "header_list": header_list,
    #                 "payload": payload
    #             }
    #             tasks.append(task)

    #     return tasks

    def send_data_location(self, file, client_addr):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.master_ip, self.master_port))
                job_data =                     {
                    "jsonrpc": "2.0",
                    "method": "data.store_data_location_client",
                    "params": {
                        "file":file,
                        "client_address":client_addr
                    },
                    "id": random.randint(1, 10000)
                }
                s.sendall(json.dumps(job_data).encode('utf-8'))
                print(f"Sent data location to MasterNode at {self.master_ip}:{self.master_port}")
        except Exception as e:
            print(f"Failed to send data location to MasterNode: {e}")

    def run_interactive_mode(self):
        print("UserClient is now running. Enter jobs interactively.")
        print("Type 'exit' to close the UserClient.")

        while True:
            input_type = input("Enter 'file' for JSON job file, 'cmd' for command-line tasks, or 'exit' to quit: ").strip().lower()

            if input_type == "exit":
                print("Exiting UserClient.")
                break
            elif input_type == "file":
                job_files = input("Enter the paths to JSON job files (comma-separated): ").strip().split(',')
                for job_file in job_files:
                    job_file = job_file.strip()
                    if os.path.exists(job_file):
                        self.handle_job_submission(job_file)
                    else:
                        print(f"Error: File '{job_file}' does not exist.")
            elif input_type == "cmd":
                tasks = []
                while True:
                    method = input("Enter the method (e.g., send_data, retrieve_data, map, reduce, or type 'done' to finish): ").strip()
                    if method.lower() == 'done':
                        break
                    key = input("Enter the key (e.g., file name or identifier): ").strip()
                    payload = input("Enter the payload (optional): ").strip() or None
                    tasks.append({"method": method, "header_list": {"key": key}, "payload": payload})
                self.handle_command_line_job(tasks)
            else:
                print("Invalid input. Please enter 'file', 'cmd', or 'exit'.")

    # def send_job(self, tasks):
    #     self.master.send_job(job=tasks)
    def send_job(self, tasks):
        """Send a job to the MasterNode."""
        print("in send job")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.master_ip, self.master_port))
                job_data =                     {
                    "jsonrpc": "2.0",
                    "method": "master.send_job",
                    "params": {
                        "tasks":tasks
                    },
                    "id": random.randint(1, 10000)
                }
                s.sendall(json.dumps(job_data).encode('utf-8'))
                print(f"Sent job to MasterNode at {self.master_ip}:{self.master_port}")
        except Exception as e:
            print(f"Failed to send job to MasterNode: {e}")

    def retrieve_data(self, header_list, conn):
        payload_type = 2
        key = header_list["key"]
        destination_port = header_list["destination_port"]
        source_port = header_list["source_port"]
        if not key or not os.path.exists(key):
            print(f"File '{key}' not found")
            status = 404
            print(f"'{key}' not found in cache.") 
        else: 
            status = 200

        payload = ""
        if status == 200: 
            payload = key

        packet = message( 
            method="retrieve_data_resp", 
            source_port=destination_port,
            destination_port=source_port,
            header_list={"key": key, "status": status, "payload_type": payload_type, "payload": payload}
        )        
        
        response = packet.process_headers()

        if status == 200:
            payloads = packet.process_payload()
            for payload in payloads:
                # print("payload: ", payload)
                decoded_payload = base64.b64decode(payload["payload"]).decode('utf-8') 
                # print("decoded payload: ", decoded_payload) # Decode base64
                json_payload = json.loads(decoded_payload)  # Parse JSON
                log_data_summary(json_payload, label="Parsed Payload on Server")
                # print("json payload: ", json_payload)
                conn.sendall((json.dumps(json_payload) + "\n").encode('utf-8'))  # Convert to JSON string and encode to bytes

                # try:
                #     decoded_payload = base64.b64decode(payload["payload"]).decode('utf-8')  # Decode base64
                #     json_payload = json.loads(decoded_payload)  # Parse JSON
                #     print("Decoded JSON payload:", json_payload)
                #     # response.update(json_payload)
                # except (base64.binascii.Error, json.JSONDecodeError) as e:
                #     print(f"Error decoding or parsing payload: {e}")
                #     continue
        # print("retrieve data response: ", response)
        return         

    def start_server(self):
        """Start a socket server to handle incoming requests."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reusing the port
        
        # Print IP and port to verify
        print(f"Binding server to IP: {self.client_ip}, Port: {self.client_port}")
        
        try:
            server_socket.bind((self.client_ip, self.client_port))
            server_socket.listen(5)
            print(f"Client server started at {self.client_ip}:{self.client_port}")
        except Exception as e:
            print(f"Failed to bind server: {e}")
            return

        while True:
            try:
                conn, addr = server_socket.accept()
                print(f"Connection received from {addr}")
                thread = threading.Thread(target=self.handle_request, args=(conn, addr), daemon=True)
                thread.start()
            except Exception as e:
                print(f"Error in server loop: {e}")

    def handle_request(self, conn, addr):
        """Handle incoming requests from other nodes."""
        file_service = FileService(self)
        dispatcher.update({
                "retrieve_data": file_service.retrieve_data
            })
        try:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                print(f"No data received from {addr}")
                return

            request = json.loads(data)
            action = request.get("method")
            print("request from client: ", request)

            if action == "retrieve_data":
                self.retrieve_data(request["params"]["header_list"], conn)
                # response = JSONRPCResponseManager.handle(json.dumps(request), dispatcher)
                # print(response)
                # print(response.json.encode('utf-8'))
                # conn.sendall(response.json.encode('utf-8'))
            else:
                print(f"Unknown action '{action}' received from {addr}")
        except Exception as e:
            print(f"Error handling request from {addr}: {e}")
        finally:
            conn.close()

    def send_file(self, conn, request):
        """Send a requested file to the requesting node."""
        print("in send file")
        file_name = ""
        if "params" in request and "header_list" in request["params"] and "key" in request["params"]["header_list"]:
            file_name = request["params"]["header_list"]["key"]
        if not file_name or not os.path.exists(file_name):
            print(f"File '{file_name}' not found")
            conn.sendall(json.dumps({"status": "error", "message": f"File '{file_name}' not found"}).encode('utf-8'))
            return

        try:
            with open(file_name, "rb") as f:
                file_data = f.read()
                conn.sendall(file_data)
            print(f"Sent file '{file_name}' to {conn.getpeername()}")
        except Exception as e:
            print(f"Error sending file '{file_name}': {e}")

    def stop_server(self):
        """Stop the server."""
        self.running = False
        print("Stopping client server...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-ip", required=True, help="MasterNode IP Address")
    parser.add_argument("--master-port", required=True, type=int, help="MasterNode Port")
    parser.add_argument("--job-files", nargs="+", type=str, help="List of job files", required=True)

    args = parser.parse_args()


    client = Client(
        master_ip=args.master_ip,
        master_port=args.master_port, 
        job_files = args.job_files
    )
    # client.start_server()
    # client.process_job()
    server_thread = threading.Thread(target=client.start_server, daemon=True)
    server_thread.start()

    # Run process_job in the main thread
    client.process_job()

    # Keep the main thread alive
    server_thread.join()

# if __name__ == '__main__':
# #     parser = argparse.ArgumentParser(description="UserClient Job Submission")
# #     parser.add_argument("--config_files", nargs="+", type=str, help="List of config files", required=True)

# #     args = parser.parse_args()

#     node = Client(args.config_files)
# #     node.run_interactive_mode()



