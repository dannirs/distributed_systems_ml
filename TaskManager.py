import threading
import time
import json
import socket
from JSONRPCDispatcher import JSONRPCDispatcher

class TaskManager:
    def __init__(self, client):
        self.current_task = None  
        self.running = True  
        self.client = client
        self.dispatcher = JSONRPCDispatcher()
        
        self.dispatcher.register_method("task.task_complete", self.task_complete)
        self.dispatcher.register_method("task.process_task", self.process_task)
        self.dispatcher.register_method("task.send_task_update", self.send_task_update)

    def task_complete(self, task_status, task_data):
        response = json.dumps({
            "jsonrpc": "2.0",
            "method": "task_update",
            "params": {"client_addr": task_data["params"]["header_list"]["source_port"], "status": task_status, "task_id": task_data["params"]["task_id"]},
            "id": 1
        })
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((self.client.master_ip, self.client.master_port))
                s.sendall(response.encode('utf-8'))
                print(f"Task update sent from {self.client.ip}:{self.client.port}")
            except ConnectionRefusedError:
                print("Failed to send task update: Master not reachable")
        self.current_task = None

    def process_task(self, task):
        self.current_task = task
        task_id = task["params"]["task_id"]
        print(f"Received task {task_id}")
        task_thread = threading.Thread(target=self.send_task_update, daemon=True)
        task_thread.start()

    def send_task_update(self):
        while self.current_task:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
                    task_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "task_update",
                        "params": {
                            "client_addr": [self.client.ip, self.client.port],
                            "status": "In Progress",
                            "task_id": self.current_task["params"]["task_id"]
                        },
                        "id": 1
                    })
                    udp_socket.sendto(task_data.encode('utf-8'), (self.client.master_ip, self.client.master_port))
                    print(f"Task update sent via UDP from {self.client.ip}:{self.client.port}")
            except Exception as e:
                print(f"Failed to send task update via UDP: {e}")

            time.sleep(10)  


    def handle_request(self, request):
        """
        Handle incoming JSON-RPC requests via the dispatcher.
        """
        response = self.dispatcher.handle_request(request)
        return response

    def stop(self):
        self.running = False

