from MasterNode import MasterNode
import json
import threading
import time
import random
import os

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
                    if node.get("is_master") and len(node) == 3:  # Master node with expected structure
                        ip = node.get("ip")
                        port = node.get("port")
                        print("MasterNode IP: ", ip)
                        print("MasterNode Port: ", port)
                        self.config_files.remove(config_file)
                        self.master = MasterNode(self.config_files, ip, port)
                        self.master.start()
                        return  # Stop searching once the master node is found
            except Exception as e:
                print(f"Error processing {config_file}: {e}")

    def handle_job_submission(self, job_file):
        print("Handling job submission")
        # Load job details from a JSON file
        with open(job_file, 'r') as file:
            job_data = json.load(file)
        
        # Generate tasks from the job and add them to the task queue
        tasks = self.create_tasks(job_data)
        print(f"Job received and tasks created and added to the queue.")
        self.send_job(tasks)

    def create_tasks(self, job_data):
        tasks = []
        job_id = random.randint(1, 40000)
        max_chunk_size = 10 * 1024 * 1024  # Example: 10 MB

        for i, dictionary in enumerate(job_data):
            method = dictionary.get("method")
            payload = dictionary.get("payload")

            if method == "send_file" and not payload:  # Check file size for send_file tasks
                file_path = dictionary["header_list"].get("file_path")
                if not os.path.exists(file_path):
                    print(f"File not found: {file_path}")
                    continue

                file_size = os.path.getsize(file_path)
                if file_size > max_chunk_size:
                    print(f"File too large, splitting: {file_path}")
                    base_name, ext = os.path.splitext(file_path)  # Extract base name and extension
                    num_chunks = -(-file_size // max_chunk_size)  # Ceiling division

                    with open(file_path, 'rb') as f:
                        for chunk_id in range(num_chunks):
                            chunk_data = f.read(max_chunk_size)
                            chunk_file_name = f"{base_name}_part{chunk_id + 1}{ext}"  # Name format
                            with open(chunk_file_name, 'wb') as chunk_file:
                                chunk_file.write(chunk_data)

                            task = {
                                "job_id": job_id,
                                "task_id": random.randint(1, 40000),
                                "method": method,
                                "header_list": {"original_file_name": file_path, "file_path": chunk_file_name},
                                "payload": None
                            }
                            tasks.append(task)
                else:
                    task = {
                        "job_id": job_id,
                        "task_id": random.randint(1, 40000),
                        "method": method,
                        "header_list": dictionary.get("header_list"),
                        "payload": payload
                    }
                    tasks.append(task)
            else:
                task = {
                    "job_id": job_id,
                    "task_id": random.randint(1, 40000),
                    "method": method,
                    "header_list": dictionary.get("header_list"),
                    "payload": payload
                }
                tasks.append(task)

        return tasks

    # def create_tasks(self, job_data):
    #     tasks = []
    #     job_id = random.randint(1, 40000)
        
    #     for i, dictionary in enumerate(job_data):
    #         task = {
    #             "job_id": job_id,
    #             "task_id": random.randint(1, 40000),
    #             "method": job_data[i]["method"],
    #             "header_list": job_data[i]["header_list"],
    #             "payload": job_data[i]["payload"]
    #         }
    #         tasks.append(task)
    #     return tasks 
    
    def send_job(self, tasks):
        self.master.send_job(tasks)

if __name__ == '__main__':
    # A server object is initialized, with a default host and port set. 
    # The server listens for 5 threads at a time. The server remains open
    # even after the connection with a client is terminated.
    # When a client thread is connected, the server goes into the handle_client() method.
    node = UserClient(["config1.json", "config4.json"])

    # Start the server in a separate thread
    # server_thread = threading.Thread(target=node.start_server)
    # server_thread.daemon = True  # Daemon thread will close automatically when main program exits
    # server_thread.start()
    # time.sleep(0.1)
    # Now, you can call send_job without waiting for the server to finish
    node.handle_job_submission("job.json")
    time.sleep(3)
    node.handle_job_submission("job2.json")

