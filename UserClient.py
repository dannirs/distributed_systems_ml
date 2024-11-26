from MasterNode import MasterNode
import json
import random
import os
import argparse

class UserClient:
    def __init__(self, config_files):
        self.master = None
        self.config_files = config_files
        self.start_master()
    
    def start_master(self):
        for config_file in self.config_files:
            try:
                with open(config_file, "r") as file:
                    configs = json.load(file)

                for node in configs:
                    if node.get("is_master") and len(node) == 3:  
                        ip = node.get("ip")
                        port = node.get("port")
                        print("MasterNode IP: ", ip)
                        print("MasterNode Port: ", port)
                        self.config_files.remove(config_file)
                        self.master = MasterNode(self.config_files, ip, port)
                        self.master.start()
                        return  
            except Exception as e:
                print(f"Error processing {config_file}: {e}")

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
        max_chunk_size = 10 * 1024 * 1024  # Chunk size in bytes (10 MB)

        for dictionary in job_data:
            method = dictionary.get("method")
            payload = dictionary.get("payload")
            header_list = dictionary.get("header_list", {})
            file_path = header_list.get("key")
            file_paths = header_list.get("keys", [])  # For mapreduce tasks

            # Handle send_data
            if method == "send_data":
                # if file_path and not os.path.exists(file_path):
                #     print(f"Invalid or missing file path: {file_path}")
                #     continue

                # Single task for sending the file
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": {"key": file_path},
                    "payload": payload
                }
                tasks.append(task)

            # Handle retrieve_data
            elif method == "retrieve_data":
                # Single task for retrieving the file
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": header_list,
                    "payload": payload
                }
                tasks.append(task)

            # Handle mapreduce
            elif method == "mapreduce":
                if not all(os.path.exists(fp) for fp in file_paths):
                    print(f"One or more files in file_paths are invalid: {file_paths}")
                    continue

                # Prepare for Map tasks and Reduce task
                map_results = []  # To store map result paths for reduce

                for file_path in file_paths:
                    file_size = os.path.getsize(file_path)

                    if file_size > max_chunk_size:  # Chunk large files
                        print(f"Chunking large file for mapreduce: {file_path}")
                        base_name, ext = os.path.splitext(file_path)
                        num_chunks = -(-file_size // max_chunk_size)

                        with open(file_path, 'rb') as f:
                            for chunk_id in range(num_chunks):
                                chunk_data = f.read(max_chunk_size)
                                chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}"

                                # Save the chunk locally
                                with open(chunk_file_name, 'wb') as chunk_file:
                                    chunk_file.write(chunk_data)

                                # Create a Map task for each chunk
                                tasks.append({
                                    "job_id": job_id,
                                    "task_id": random.randint(1, 40000),
                                    "method": "map",
                                    "header_list": {"original_file": file_path, "key": chunk_file_name},
                                    "payload": None
                                })
                                map_results.append(chunk_file_name)  # Add chunk name to map results
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

        return tasks


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
        self.master.send_job(job=tasks)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="UserClient Job Submission")
    parser.add_argument("--config_files", nargs="+", type=str, help="List of config files", required=True)

    args = parser.parse_args()

    node = UserClient(args.config_files)
    node.run_interactive_mode()
