import socket
import json
import random

class JobManager:
    def __init__(self, master):
        self.master = master    
        self.task_queue = []          
        self.tasks_pending_results = {}

    def submit_job(self, job=None, **kwargs):
        """
        Process job submission and assign tasks.
        Accepts job data directly or via JSON-RPC keyword arguments.
        """
        job = job or kwargs.get("job")
        print("JobManager is processing the job")
        self.task_queue = job
        self.assign_tasks()

    def get_available_clients(self, server_address):
        """
        Get a list of available clients for the given server.
        """
        available_clients = []
        for value in self.master.client_to_server_registry.values():
            for client in value:
                client_info = self.master.client_registry.get(client, {})
                if client_info.get("status") and not client_info.get("task_status"):
                    available_clients.append(client)
        return available_clients

    def send_task_to_client(self, server_ip, server_port, client_address, task):
        """
        Send a task to a client via its assigned server.
        """
        print("Sending task:", task, "to server:", (server_ip, server_port), "for client:", client_address)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((server_ip, server_port))
                jsonrpc = "2.0"
                message = {
                    "jsonrpc": jsonrpc,
                    "method": "send_task_to_client",
                    "params": {
                        "client_address": client_address,
                        "task_data": task
                    },
                    "id": random.randint(1, 40000)
                }
                sock.sendall(json.dumps(message).encode('utf-8'))
                print(f"Task {task['task_id']} sent to {client_address} via {server_ip}:{server_port}")
                response = sock.recv(1024).decode('utf-8')
                print("Response from client:", response)
                self.master.client_registry[client_address]["task_status"] = False
                self.task_queue.pop(0)
                self.tasks_pending_results[task["task_id"]] = task
        except Exception as e:
            print(f"Failed to send task {task['task_id']} to {client_address}: {e}")

    def assign_tasks(self):
        """
        Assign tasks to available clients.
        """
        while self.task_queue:
            for server_address in self.master.server_registry:
                server_ip, server_port = server_address
                available_clients = self.get_available_clients(server_address)

                if not available_clients:
                    continue

                for client_address in available_clients:
                    if self.task_queue:
                        task = self.task_queue[0]
                        self.send_task_to_client(server_ip, server_port, client_address, task)
        print("All tasks have been assigned.")

    def get_task_response(self, response=None, **kwargs):
        """
        Process the response from a client after task execution.
        """
        response = response or kwargs.get("response")
        print("Task response:", response)
        client_info = self.master.client_registry.get((response["params"]["client_addr"][0], response["params"]["client_addr"][1]), {})
        if client_info:
            client_info["task_status"] = False
        if response["params"]["status"] == "404":
            self.task_queue.append(self.tasks_pending_results[response["params"]["task_id"]])
            self.assign_tasks()
        else:
            print("Tasks pending results:", self.tasks_pending_results)
            self.tasks_pending_results.pop(response["params"]["task_id"], None)
