import threading
import time
import random
from queue import Queue
import json
import socket

class TaskManager:
    def __init__(self, client):
        self.current_task = None  # Tracks the current task being processed
        self.running = True  # Controls the operation loop
        self.client = client

    def task_complete(self, task_status, task_data):
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #     s.connect((self.master_ip, self.master_port))
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
        # Wait for a task from the JobManager
        self.current_task = task
        task_id = task["task_id"]
        print(f"Received task {task_id}")
        task_thread = threading.Thread(target=self.send_task_update, daemon=True)
        task_thread.start()
        self.execute_task(task_id, task["task_data"])

    def send_task_update(self):
         while self.current_task:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.connect((self.master_ip, self.master_port))
                    task_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "task_update",
                        "params": {"worker_ip": self.ip, "worker_port": self.port, "status": "In Progress"},
                        "id": 1
                    })
                    s.sendall(task_data.encode('utf-8'))
                    print(f"Task update sent from {self.ip}:{self.port}")
                except ConnectionRefusedError:
                    print("Failed to send task update: Master not reachable")
            time.sleep(10)  # Wait 5 seconds before sending the next heartbeat

    def stop(self):
        self.running = False

