import threading
import time
import json
import socket

class TaskManager:
    def __init__(self, client):
        self.current_task = None  
        self.running = True  
        self.client = client

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
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.connect((self.client.master_ip, self.client.master_port))
                    task_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "task_update",
                        "params": {"client_addr": [self.client.ip, self.client.port], "status": "In Progress", "task_id": self.current_task["params"]["task_id"]},
                        "id": 1
                    })
                    s.sendall(task_data.encode('utf-8'))
                    print(f"Task update sent from {self.client.ip}:{self.client.port}")
                except ConnectionRefusedError:
                    print("Failed to send task update: Master not reachable")
            time.sleep(10)  

    def stop(self):
        self.running = False

