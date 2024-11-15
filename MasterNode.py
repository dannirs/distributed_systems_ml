import socket
import threading
import json
from JobManager import JobManager
from WorkerClient import WorkerClient
from WorkerServer import WorkerServer
import time
from Worker import Worker

class MasterNode:
    def __init__(self, config_files, ip="localhost", port=5678):
        self.ip = ip
        self.port = port
        self.server_registry = {}       # Track registered workers
        self.client_registry = {}
        self.client_to_server_registry = {}
        self.data_registry = {}         # Track data location on workers
        self.job_manager = JobManager(self)  # JobManager for task distribution
        self.config_files = config_files
        self.heartbeat_timeout = 15
        self.inactive_workers = {}

    def start_workers(self):
        """
        Iterates through all the configuration files to:
        - Start WorkerServer nodes.
        - Create a dictionary for WorkerClient nodes.
        """
        print("Starting WorkerNodes")
        for config_file in self.config_files:
            try:
                with open(config_file, "r") as file:
                    configs = json.load(file)
                for node in configs:
                    if node.get("role") == "WorkerServer":
                        worker = Worker(self.ip, self.port, node.get("ip"), node.get("port"), configs)
                        threading.Thread(target=worker.start, daemon=True).start()
                        server_clients = worker.get_clients()
                        self.server_registry[(node.get("ip"), node.get("port"))] = {"status": True}
                        self.client_to_server_registry[server_clients[0]] = server_clients[1]
                        for i, dictionary in enumerate(server_clients[1]):
                            self.client_registry[server_clients[1][i]] = {
                                                                        "status": True,
                                                                        "task_status": False
                                                                    }
                        break
            except Exception as e:
                print(f"Error processing {config_file}: {e}")
            print("Finished initializing")

    # def add_worker_server(self, server_config):
    #     """
    #     Starts a WorkerServer node.
    #     """
    #     address = f"{server_config['ip']}:{server_config['port']}"
    #     if address not in self.client_to_server_registry: 
    #         self.client_to_server_registry[address] = []
    #     # print("Starting WorkerNode's server")
    #     # address = f"{server_config['ip']}:{server_config['port']}"
    #     # self.server_registry.append(address)
    #     # print(f"Started WorkerServer at {address}")

    #     # # Simulating a server start using threading
    #     # threading.Thread(target=self.simulate_server, args=(server_config,), daemon=True).start()

    # def add_worker_client(self, client_config):
    #     """
    #     Adds a WorkerClient node to the dictionary.
    #     """
    #     print("Adding WorkerNode's client to the registry")
    #     client_address = f"{client_config['ip']}:{client_config['port']}"
    #     server_address = f"{client_config['server_ip']}:{client_config['server_port']}"
    #     self.worker_clients[client_address] = {
    #         "availability": True,
    #         "task_status": None
    #     }
    #     if server_address not in self.client_to_server_registry:
    #         self.client_to_server_registry[server_address] = [client_address]
    #     else:
    #         self.client_to_server_registry[server_address].append(client_address)

    #     print(f"Added WorkerClient at {client_address} linked to {server_address}")

    def start(self):
        print(f"Master node initializing on {self.ip}:{self.port}")
        self.start_workers()
        def server_thread():
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.ip, self.port))
            server_socket.listen(5)
            print(f"Master node listening on {self.ip}:{self.port}")

            # Accept connections in the server thread
            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()

        # Run the server in a separate thread
        threading.Thread(target=server_thread, daemon=True).start()

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
        elif method == "task_update":
            self.job_manager.get_task_response(request_data["params"])
        conn.close()

    # def start_workers(self):
    #     print("Starting WorkerNodes")
    #     for i, dictionary in enumerate(self.config):
    #         print(self.config[i]["node_type"])
    #         if not self.config[i]["is_master"]:
    #             key = (self.config[i]["ip"], self.config[i]["port"])
    #             ip = self.config[i]["ip"]
    #             port = self.config[i]["port"]
    #             role = self.config[i]["role"]
    #             self.worker_registry[key] = {
    #                     "ip": ip,
    #                     "status": "active",
    #                     "role": role,
    #                     "busy": False}
    #             if role == "WorkerClient":
    #                 print("Initializing WorkerClient")
    #                 client = WorkerClient(self.ip, self.port, ip, port)
    #                 threading.Thread(target=client.start_listening).start()
    #             elif role == "WorkerServer":
    #                 print("Initializing WorkerServer")
    #                 server = WorkerServer(self.ip, self.port, ip, port)
    #                 threading.Thread(target=server.start_server).start()

    def receive_heartbeat(self, params):
        worker_ip = params["worker_ip"]
        if worker_ip not in self.client_registry and worker_ip in self.inactive_workers:
            self.client_registry[worker_ip] = self.inactive_workers[worker_ip]
            del self.inactive_workers[worker_ip]
        if worker_ip in self.client_registry:
            # Update the last heartbeat time for this worker
            self.client_registry[worker_ip]["last_heartbeat"] = time.time()
            self.client_registry[worker_ip]["status"] = True
            print(f"Heartbeat received from worker {worker_ip}")

    def monitor_worker_heartbeats(self):
        # Periodically checks each worker's last heartbeat time
        while True:
            current_time = time.time()
            for worker_ip, info in self.client_registry.items():
                last_heartbeat = info["last_heartbeat"]
                if info["status"] == True and current_time - last_heartbeat > self.heartbeat_timeout:
                    # Mark the worker as inactive if the timeout has passed
                    self.client_registry[worker_ip]["status"] = False
                    self.inactive_workers[worker_ip] = self.client_registry[worker_ip]
                    del self.client_registry[worker_ip]
                    print(f"Worker {worker_ip} marked as inactive (no heartbeat)")
            time.sleep(15)  # Check every 5 seconds

    def get_data_location(self, conn, data_key):
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
        self.job_manager.handle_job_submission(job)