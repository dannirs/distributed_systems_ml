import socket
import json
import random

class JobManager:
    def __init__(self, master):
        self.master = master
        self.task_queue = []
        self.tasks_pending_results = {}
        self.reduce_task = None  # Store the Reduce task for later

    def submit_job(self, tasks=None, **kwargs):
        tasks = tasks or kwargs.get("job")
        print("JobManager is processing the job")
        # Separate tasks into MapReduce and others
        map_tasks = [task for task in tasks if task["method"] == "map"]
        reduce_tasks = [task for task in tasks if task["method"] == "reduce"]
        other_tasks = [task for task in tasks if task["method"] not in ["map", "reduce"]]
        # Add tasks to the queue
        self.task_queue.extend(map_tasks + other_tasks)
        # Store the Reduce task(s) for later execution
        self.reduce_task = reduce_tasks[0] if reduce_tasks else None
        # Assign tasks to available clients
        self.assign_tasks()


    def get_available_clients(self, server_address):
        available_clients = []

        # Safely access clients for the given server
        clients = self.master.client_to_server_registry.get(server_address, [])

        # Iterate over the client list
        for client in clients:  # Each client is a list like ['localhost', 5684]
            client_info = self.master.client_registry.get((client[0], client[1]), {})
            if client_info.get("status") and not client_info.get("task_status"):
                available_clients.append((client[0], client[1]))

        return available_clients

    def send_task_to_client(self, server_ip, server_port, client_address, task):
        print("Sending task:", task, "to server:", (server_ip, server_port), "for client:", client_address)
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((server_ip, server_port))
                message = {
                    "jsonrpc": "2.0",
                    "method": "send_task_to_client",
                    "params": {"header_list": {
                        "client_address": client_address,
                        "task_data": task,
                        "finished": True
                        }
                    },
                    "id": random.randint(1, 40000)
                }
                sock.sendall(json.dumps(message).encode("utf-8"))
                print(f"Task {task['task_id']} sent to {client_address} via {server_ip}:{server_port}")
                self.tasks_pending_results[task["task_id"]] = task
                self.task_queue.pop(0)
                response = sock.recv(1024).decode("utf-8")
                self.master.client_registry[client_address]["task_status"] = False
        except Exception as e:
            print(f"Failed to send task {task['task_id']} to {client_address}: {e}")

    def assign_tasks(self):
        while self.task_queue:
            for server_address in self.master.server_registry:
                server_ip, server_port = server_address
                available_clients = self.get_available_clients(server_address)
                if not available_clients:
                    continue

                for client_address in available_clients:
                    if self.task_queue:
                        task = self.task_queue[0]

                        # Send task to client
                        self.send_task_to_client(server_ip, server_port, client_address, task)

    def assign_reduce_task(self):
        for server_address in self.master.server_registry:
            server_ip, server_port = server_address
            available_clients = self.get_available_clients(server_address)

            if available_clients:
                client_address = available_clients[0]
                self.send_task_to_client(server_ip, server_port, client_address, self.reduce_task)
                self.reduce_task = None  # Clear the Reduce task once assigned
                break

    def get_task_response(self, response=None, **kwargs):
        response = response or kwargs.get("response")
        client_info = self.master.client_registry.get((response["client_addr"][0], response["client_addr"][1]), {})
        if client_info:
            client_info["task_status"] = False
        if response["status"] == "404":
            # Reassign failed task
            self.task_queue.append(self.tasks_pending_results[response["task_id"]])
            self.assign_tasks()
        else:
            self.tasks_pending_results.pop(response["task_id"], None)

        # Check if all map tasks are completed
        if not self.tasks_pending_results and not self.task_queue and self.reduce_task:
            self.assign_reduce_task()