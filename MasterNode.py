import socket
import threading
import json
from JobManager import JobManager
from WorkerClient import WorkerClient
from WorkerServer import WorkerServer
import time

class MasterNode:
    def __init__(self, config, ip="localhost", port=5678):
        self.ip = ip
        self.port = port
        self.worker_registry = {}       # Track registered workers
        self.data_registry = {}         # Track data location on workers
        self.job_manager = JobManager(self)  # JobManager for task distribution
        self.config = config
        self.heartbeat_timeout = 15
        self.inactive_workers = {}

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
            self.get_data_location(conn, request_data["params"]["key"])
        elif method == "store_data_location":
            self.store_data_location(conn, request_data["params"]["key"], request_data["params"]["address"])
        conn.close()

    def start_workers(self):
        print("In start_workers()")
        for i, dictionary in enumerate(self.config):
            print(self.config[i]["node_type"])
            if not self.config[i]["is_master"]:
                key = (self.config[i]["ip"], self.config[i]["port"])
                ip = self.config[i]["ip"]
                port = self.config[i]["port"]
                role = self.config[i]["role"]
                self.worker_registry[key] = {
                        "ip": ip,
                        "status": "active",
                        "role": role,
                        "busy": False}
                if role == "WorkerClient":
                    print("Initializing WorkerClient")
                    client = WorkerClient(self.ip, self.port, ip, port)
                    threading.Thread(target=client.start_listening).start()
                elif role == "WorkerServer":
                    print("Initializing WorkerServer")
                    server = WorkerServer(self.ip, self.port, ip, port)
                    threading.Thread(target=server.start_server).start()

    def receive_heartbeat(self, params):
        worker_ip = params["worker_ip"]
        if worker_ip not in self.worker_registry and worker_ip in self.inactive_workers:
            self.worker_registry[worker_ip] = self.inactive_workers[worker_ip]
            del self.inactive_workers[worker_ip]
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
                    self.inactive_workers[worker_ip] = self.worker_registry[worker_ip]
                    del self.worker_registry[worker_ip]
                    print(f"Worker {worker_ip} marked as inactive (no heartbeat)")
            time.sleep(15)  # Check every 5 seconds

    def get_data_location(self, conn, data_key):
        print("In get_data_location")
        # Retrieve the location of data based on data_key
        worker_info = self.data_registry.get(data_key)
        if worker_info:
            response = {"jsonrpc": "2.0", "result": worker_info, "id": 3}
        else:
            response = {"jsonrpc": "2.0", "error": "Data not found", "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def store_data_location(self, conn, data, address):
        self.data_registry[data] = address
        response = {"jsonrpc": "2.0", "result": "Success", "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def send_job(self, job):
        print("In send_job()")
        self.job_manager.handle_job_submission(job)