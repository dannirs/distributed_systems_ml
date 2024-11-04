import socket
import threading
import json
from JobManager import JobManager
from WorkerClient import WorkerClient
from WorkerServer import WorkerServer
import time

class MasterNode:
    def __init__(self, config, ip="localhost", port=5678):
        self.master_ip = ip
        self.master_port = port
        self.worker_registry = {}       # Track registered workers
        self.data_registry = {}         # Track data location on workers
        self.job_manager = JobManager(self)  # JobManager for task distribution
        self.config = config
        self.heartbeat_timeout = 5

    def start(self):
        # Start the master server to accept worker connections and handle user jobs
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        print(f"Master node listening on {self.ip}:{self.port}")
        self.start_workers()

        # Continuously accept connections from workers and clients
        while True:
            conn, addr = server_socket.accept()
            threading.Thread(target=self.handle_connection, args=(conn,)).start()

    def handle_connection(self, conn):
        # Process incoming connections (either from a worker or a client)
        request = conn.recv(1024).decode('utf-8')
        request_data = json.loads(request)

        # Dispatch request based on the specified method
        method = request_data.get("method")
        if method == "heartbeat":
            self.receive_heartbeat(request_data["params"])
        elif method == "submit_job":
            self.job_manager.handle_job_submission(request_data["params"])
        elif method == "get_data_location":
            self.send_data_location(conn, request_data["params"]["data_key"])
        conn.close()

    def start_workers(self):
        for node in self.config:
            if node != "master":
                worker_ip = self.config[node]["worker_ip"]
                worker_port = self.config[node]["worker_port"]
                node_type = self.config[node]["node_type"]
                self.worker_registry[worker_ip] = {
                        "port": worker_port,
                        "status": "active",
                        "node_type": node_type,
                        "busy": False}
                if node_type == "client":
                    client = WorkerClient(self.master_ip, self.master_port, worker_ip, worker_port)
                    threading.Thread(target=client.send_heartbeat).start()
                elif node_type == "server":
                    server = WorkerServer(self.master_ip, self.master_port, worker_ip, worker_port)
                    threading.Thread(target=server.send_heartbeat).start()

    def receive_heartbeat(self, params):
        worker_ip = params["worker_ip"]
        if worker_ip in self.worker_registry:
            # Update the last heartbeat time for this worker
            self.worker_registry[worker_ip]["last_heartbeat"] = time.time()
            self.worker_registry[worker_ip]["status"] = "active"
            print(f"Heartbeat received from worker {worker_ip}")

    def monitor_worker_heartbeats(self):
        # Periodically checks each worker's last heartbeat time
        while True:
            current_time = time.time()
            for worker_ip, info in self.worker_registry.items():
                last_heartbeat = info["last_heartbeat"]
                if info["status"] == "active" and current_time - last_heartbeat > self.heartbeat_timeout:
                    # Mark the worker as inactive if the timeout has passed
                    self.worker_registry[worker_ip]["status"] = "inactive"
                    print(f"Worker {worker_ip} marked as inactive (no heartbeat)")
            time.sleep(5)  # Check every 5 seconds

    def send_data_location(self, conn, data_key):
        # Retrieve the location of data based on data_key
        worker_info = self.data_registry.get(data_key)
        if worker_info:
            response = {"jsonrpc": "2.0", "result": worker_info, "id": 3}
        else:
            response = {"jsonrpc": "2.0", "error": "Data not found", "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))
