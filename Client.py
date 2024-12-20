import json
import random
import os
import argparse
import csv
import socket
import threading
import json
from message import message
import base64
from handlers import log_data_summary

class Client:
    def __init__(self, master_ip, master_port, job_files, client_ip="localhost", client_port=2001):
        self.master_ip = master_ip
        self.master_port = master_port
        self.client_ip = client_ip
        self.client_port = client_port
        self.files = []
        self.job_files = job_files
    
    def process_job(self):
        for job_file in self.job_files:
            job_file = job_file.strip()
            if os.path.exists(job_file):
                self.handle_job_submission(job_file)
            else:
                print(f"Error: File '{job_file}' does not exist.")

    def handle_job_submission(self, job_file):
        print(f"Processing job file: {job_file}\n")
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
            file_paths = header_list.get("keys", [])

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

            elif method == "retrieve_data":
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": header_list,
                    "payload": payload
                }
                tasks.append(task)

            elif method == "mapreduce" :
                if not all(os.path.exists(fp) for fp in file_paths):
                    print(f"One or more files in file_paths are invalid: {file_paths}")
                    continue

                map_results = []  

                for file_path in file_paths:
                    file_size = os.path.getsize(file_path)
                    if file_size > max_chunk_size: 
                        print(f"Chunking large file for mapreduce: {file_path}")
                        base_name, ext = os.path.splitext(file_path)
                        chunk_id = 0

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
            else:
                print(f"Unsupported method: {method}")
        for file in self.files:
            self.send_data_location(file, (self.client_ip, self.client_port))
        return tasks

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

    def send_job(self, tasks):
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
                decoded_payload = base64.b64decode(payload["payload"]).decode('utf-8') 
                json_payload = json.loads(decoded_payload)  # Parse JSON
                conn.sendall((json.dumps(json_payload) + "\n").encode('utf-8'))  # Convert to JSON string and encode to bytes
        return         

    def start_server(self):
        """Start a socket server to handle incoming requests."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Allow reusing the port
        
        # Print IP and port to verify
        print(f"Binding server to IP: {self.client_ip}, Port: {self.client_port}\n")
        
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
        try:
            data = conn.recv(1024).decode('utf-8')
            if not data:
                print(f"No data received from {addr}")
                return

            request = json.loads(data)
            action = request.get("method")

            if action == "retrieve_data":
                self.retrieve_data(request["params"]["header_list"], conn)
            else:
                print(f"Unknown action '{action}' received from {addr}")
        except Exception as e:
            print(f"Error handling request from {addr}: {e}")
        finally:
            conn.close()

    def stop_server(self):
        """Stop the server."""
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
    server_thread = threading.Thread(target=client.start_server, daemon=True)
    server_thread.start()

    # Run process_job in the main thread
    client.process_job()

    # Keep the main thread alive
    server_thread.join()
