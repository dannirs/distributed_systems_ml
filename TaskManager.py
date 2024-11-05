import socket
import json
import threading

class TaskManager:
    def __init__(self, worker_ip, worker_port, master_ip, master_port):
        self.worker_ip = worker_ip
        self.worker_port = worker_port
        self.master_ip = master_ip
        self.master_port = master_port
        self.local_data_store = {}  # Simulated local storage for this worker

    def start_listening(self):
        # Start listening for tasks from the master node
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.worker_ip, self.worker_port))
        server_socket.listen(5)
        print(f"TaskManager listening on {self.worker_ip}:{self.worker_port}")

        while True:
            conn, addr = server_socket.accept()
            task_thread = threading.Thread(target=self.handle_task, args=(conn,))
            task_thread.start()

    def handle_task(self, conn):
        # Handle incoming task from the master
        request = conn.recv(1024).decode('utf-8')
        task_data = json.loads(request)
        print(f"Received task: {task_data}")

        task_type = task_data["method"]
        params = task_data["params"]

        # Execute the task based on its type
        if task_type == "store_data":
            result = self.store_data(params["data_key"], params["data"])
        elif task_type == "retrieve_data":
            result = self.retrieve_data(params["data_key"])
        else:
            result = {"status": "error", "message": f"Unknown task type {task_type}"}

        # Send the result back to the master
        response = json.dumps({"result": result})
        conn.sendall(response.encode('utf-8'))
        conn.close()

    def store_data(self, data_key, data):
        # Simulate storing data locally
        self.local_data_store[data_key] = data
        print(f"Data stored for key {data_key}")
        return {"status": "success", "message": f"Data stored for key {data_key}"}

    def retrieve_data(self, data_key):
        # Simulate retrieving data from local storage
        data = self.local_data_store.get(data_key, None)
        if data is not None:
            print(f"Data retrieved for key {data_key}")
            return {"status": "success", "data": data}
        else:
            print(f"No data found for key {data_key}")
            return {"status": "error", "message": "Data not found"}
