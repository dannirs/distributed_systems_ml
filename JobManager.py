import socket
import json
import random

class JobManager:
    def __init__(self, master):
        self.master = master    
        self.task_queue = []          
        self.tasks_pending_results = {}        

    def handle_job_submission(self, job):
        print("JobManager is processing the job")
        self.task_queue = job
        self.assign_tasks()

    def get_available_clients(self, server_address):
        available_clients = []
        for value in self.master.client_to_server_registry.values():
            for client in value:
                if self.master.client_registry[client]["status"] == True and self.master.client_registry[client]["task_status"] == False:
                    available_clients.append(client)
        return available_clients

    def send_task_to_client(self, server_ip, server_port, client_address, task):
        """
        Sends a task to the specified client via the server.
        """
        print("Sending task: ", task, " to server: ", (server_ip, server_port), " for client: ", client_address)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((server_ip, server_port))
                jsonrpc = "2.0"
                id = random.randint(1, 40000)
                params = {
                    "client_address": client_address,
                    "task_data": task
                }
                message =   {
                                "jsonrpc": jsonrpc,
                                "method": "send_task_to_client",
                                "params": params,
                                "id": id
                            }
                sock.sendall(json.dumps(message).encode('utf-8'))
                print(f"Task {task['task_id']} sent to {client_address} via {server_ip}:{server_port}")
                response = sock.recv(1024).decode('utf-8')
                self.master.client_registry[client_address]["task_status"] = False
                self.task_queue.pop(0)
                self.tasks_pending_results[task["task_id"]] = task

        except Exception as e:
            print(f"Failed to send task {task['task_id']} to {client_address}: {e}")

    def assign_tasks(self):
        """
        Iterates through tasks and assigns them to available clients via their servers.
        """
        task_index = 0
        while self.task_queue:
            for server_address in self.master.server_registry:
                server_ip = server_address[0]
                server_port = server_address[1]
                available_clients = self.get_available_clients(server_address)

                if not available_clients:
                    continue

                for client_address in available_clients:
                    # if task_index >= len(self.task_queue):
                    #     break
                    while len(self.task_queue) > 0:
                        task = self.task_queue[0]
                        self.send_task_to_client(server_ip, server_port, client_address, task)
                    # task_index += 1

                # if task_index >= len(self.task_queue):
                #     break

        print("All tasks have been assigned.")

    def get_task_response(self, response):
        self.master.client_registry[(response["client_addr"][0], response["client_addr"][1])]["task_status"] = False

        # self.master.client_registry[{response["client_addr"][0], response["client_addr"][1]}]["task_status"] = False
        del self.task_pending_results[response["params"]["task_id"]]
        if response["params"]["status"] == "404":
            self.task_queue.append(self.task_pending_results[response["params"]["task_id"]])
            self.assign_tasks()

    # def handle_job_submission(self, job_file):
    #     for port in self.master.worker_registry: 
    #         if self.master.worker_registry[port]["role"] == "WorkerClient" and self.master.worker_registry[port]["busy"] == False:
    #             self.worker_status[port] = "available"
    #         else:
    #             self.worker_status[port] = "busy"
    #             self.servers.append(port)
    #     # Load job details from a JSON file
    #     with open(job_file, 'r') as file:
    #         job_data = json.load(file)
        
    #     # Generate tasks from the job and add them to the task queue
    #     tasks = self.create_tasks(job_data)
    #     self.task_queue.extend(tasks)
    #     print(f"Job received and {len(tasks)} tasks created and added to the queue.")
    #     self.assign_tasks()

    # def create_tasks(self, job_data):
    #     tasks = []
    #     job_id = random.randint(1, 40000)
        
    #     for i, dictionary in enumerate(job_data):
    #         server = self.servers[i % len(self.servers)]
    #         job_data[i]["header_list"]["destination_port"] = server
    #         task = {
    #             "job_id": job_id,
    #             "task_id": random.randint(1, 40000),
    #             "method": job_data[i]["method"],
    #             "header_list": job_data[i]["header_list"],
    #             "payload": job_data[i]["payload"]
    #         }
    #         tasks.append(task)
        
    #     print(tasks)
    #     return tasks
    
    # def assign_tasks(self):
    #     # Assign tasks to available workers
    #     print("In assign_tasks()")
    #     for task in list(self.task_queue):  # Use list() to avoid modifying while iterating
    #         if self.has_available_worker():
    #             worker = self.get_available_worker()
    #             self.send_task_to_client(worker, task)
    #             self.task_queue.remove(task)

    # def has_available_worker(self):
    #     # Check if there is an available worker
    #     return any(status == "available" for status in self.worker_status.values())

    # def get_available_worker(self):
    #     # Get the first available worker
    #     for worker, status in self.worker_status.items():
    #         if status == "available":
    #             self.worker_status[worker] = "busy"  # Mark worker as busy
    #             return worker

    # def send_task_to_client(self, worker, task):
    #     # Simulate sending a task to a worker
    #     worker_ip, worker_port = worker
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #         s.connect((worker_ip, worker_port))
    #         request = {
    #             "jsonrpc": "2.0",
    #             "method": task["method"],
    #             "params": task,
    #             "id": 1
    #         }
    #         print("Sending task to client: ", request)
    #         s.sendall(json.dumps(request).encode('utf-8'))
    #         response = s.recv(1024).decode('utf-8')
    #         print("response: ", response)
    #         print(f"Task {task['task_id']} result from worker {worker}: {response}")
    #         response = json.loads(response)
    #         print("jobmanager got response: ", response)
    #         # After sending the task, mark it as completed
    #         self.task_results[task["task_id"]] = response["params"]["status"]
    #         # Mark worker as available again
    #         self.worker_status[worker] = "available"
