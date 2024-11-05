import socket
import json
import random

class JobManager:
    def __init__(self, master):
        self.master = master
        self.worker_status = {}       # Track worker availability
        self.task_queue = []          # Queue to hold tasks generated from jobs
        self.task_results = {}        # Track completed tasks
        self.servers = []

    def handle_job_submission(self, job_file):
        for port in self.master.worker_registry: 
            if self.master.worker_registry[port]["role"] == "WorkerClient" and self.master.worker_registry[port]["busy"] == False:
                self.worker_status[port] = "available"
            else:
                self.worker_status[port] = "busy"
                self.servers.append(port)
        # Load job details from a JSON file
        with open(job_file, 'r') as file:
            job_data = json.load(file)
        
        # Generate tasks from the job and add them to the task queue
        tasks = self.create_tasks(job_data)
        self.task_queue.extend(tasks)
        print(f"Job received and {len(tasks)} tasks created and added to the queue.")
        self.assign_tasks()

    def create_tasks(self, job_data):
        # Convert the job into a list of individual tasks
        tasks = []
        job_id = random.randint(1, 40000)
        
        # Example job_data structure:
        # job_data = {
        #     "job_id": "job1",
        #     "tasks": [
        #         {"task_id": "task1", "task_type": "store_data", "data_key": "file1", "data": "data1", "dependencies": []},
        #         {"task_id": "task2", "task_type": "retrieve_data", "data_key": "file1", "dependencies": ["task1"]},
        #         ...
        #     ]
        # }
        
        for i, dictionary in enumerate(job_data):
            server = self.servers[i % len(self.servers)]
            job_data[i]["header_list"]["destination_port"] = server
            task = {
                "job_id": job_id,
                "task_id": random.randint(1, 40000),
                "method": job_data[i]["method"],
                "header_list": job_data[i]["header_list"],
                "payload": job_data[i]["payload"]
            }
            tasks.append(task)
        
        print(tasks)
        return tasks
    
    def assign_tasks(self):
        # Assign tasks to available workers
        print("In assign_tasks()")
        for task in list(self.task_queue):  # Use list() to avoid modifying while iterating
            if self.has_available_worker():
                worker = self.get_available_worker()
                self.send_task_to_client(worker, task)
                self.task_queue.remove(task)

    def has_available_worker(self):
        # Check if there is an available worker
        return any(status == "available" for status in self.worker_status.values())

    def get_available_worker(self):
        # Get the first available worker
        for worker, status in self.worker_status.items():
            if status == "available":
                self.worker_status[worker] = "busy"  # Mark worker as busy
                return worker

    def send_task_to_client(self, worker, task):
        # Simulate sending a task to a worker
        worker_ip, worker_port = worker
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((worker_ip, worker_port))
            request = {
                "jsonrpc": "2.0",
                "method": task["method"],
                "params": task,
                "id": 1
            }
            print("Sending task to client: ", request)
            s.sendall(json.dumps(request).encode('utf-8'))
            response = s.recv(1024).decode('utf-8')
            print("response: ", response)
            print(f"Task {task['task_id']} result from worker {worker}: {response}")
            response = json.loads(response)
            print("jobmanager got response: ", response)
            # After sending the task, mark it as completed
            self.task_results[task["task_id"]] = response["params"]["status"]
            # Mark worker as available again
            self.worker_status[worker] = "available"
