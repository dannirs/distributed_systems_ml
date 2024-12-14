import socket
import threading
import json
from JobManager import JobManager
import time
from Worker import Worker
from DataManager import DataManager
from JSONRPCProxy import JSONRPCProxy
from JSONRPCDispatcher import JSONRPCDispatcher

class MasterNode:
    def __init__(self, ip="localhost", port=5678):
        self.ip = ip
        self.port = port if port else self.get_available_port()
        print(f"MasterNode starting on IP: {self.ip} Port: {self.port}")
        self.start()
        # self.port = port
        self.server_registry = {}       
        self.client_registry = {}
        self.client_to_server_registry = {}
        self.job_manager = JobManager(self)  
        # self.config_files = config_files
        self.heartbeat_timeout = 15
        self.inactive_workers = {}
        self.data_manager = DataManager()
        self.dispatcher = JSONRPCDispatcher()

        # Register methods in dispatcher
        self.dispatcher.register_method("job.submit_job", self.job_manager.submit_job)
        self.dispatcher.register_method("job.get_task_response", self.job_manager.get_task_response)
        self.dispatcher.register_method("job.get_available_clients", self.job_manager.get_available_clients)
        self.dispatcher.register_method("job.send_task_to_client", self.job_manager.send_task_to_client)
        self.dispatcher.register_method("job.assign_tasks", self.job_manager.assign_tasks)
        self.dispatcher.register_method("data.get_data_location", self.data_manager.get_data_location)
        self.dispatcher.register_method("data.store_data_location", self.data_manager.store_data_location)
        self.dispatcher.register_method("master.receive_heartbeat", self.receive_heartbeat)
        self.dispatcher.register_method("master.receive_node_request", self.receive_node_request)
        self.dispatcher.register_method("master.send_job", self.send_job)

        # Wrap managers with JSONRPCProxy
        self.job_manager_proxy = JSONRPCProxy(self.dispatcher, prefix="job")
        self.data_manager_proxy = JSONRPCProxy(self.dispatcher, prefix="data")

    def get_available_port(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print("binding")
            s.bind(("", 0))  # Bind to any available port
            return s.getsockname()[1]  # Return the port number

    def receive_node_request(self, ip, port, clients):
        print(ip)
        self.server_registry[(ip, port)] = {"status": True}
        self.client_to_server_registry[clients[0]] = clients[1]
        for i, dictionary in enumerate(clients[1]):
            self.client_registry[clients[1][i]] = {
                                                        "status": True,
                                                        "task_status": False
                                                    }


    # def start_workers(self):
    #     print("Starting WorkerNodes")
    #     for config_file in self.config_files:
    #         try:
    #             with open(config_file, "r") as file:
    #                 configs = json.load(file)
    #             for node in configs:
    #                 if node.get("role") == "WorkerServer":
    #                     worker = Worker(self.ip, self.port, node.get("ip"), node.get("port"), configs)
    #                     threading.Thread(target=worker.start, daemon=True).start()
    #                     server_clients = worker.get_clients()
    #                     self.server_registry[(node.get("ip"), node.get("port"))] = {"status": True}
    #                     self.client_to_server_registry[server_clients[0]] = server_clients[1]
    #                     for i, dictionary in enumerate(server_clients[1]):
    #                         self.client_registry[server_clients[1][i]] = {
    #                                                                     "status": True,
    #                                                                     "task_status": False
    #                                                                 }
    #                     break
    #         except Exception as e:
    #             print(f"Error processing {config_file}: {e}")
    #         print("Finished initializing")

    def start(self):
        # print(f"Master node initializing on {self.ip}:{self.port}")
        # self.start_workers()
        def server_thread():
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.bind((self.ip, self.port))
            server_socket.listen(5)
            print(f"Master node listening on {self.ip}:{self.port}")

            while True:
                conn, addr = server_socket.accept()
                threading.Thread(target=self.handle_connection, args=(conn,), daemon=True).start()
        print(self.ip)
        print(self.port)
        threading.Thread(target=server_thread, daemon=True).start()
        print("Master node listening")

    def handle_connection(self, conn):
        """
        Handle incoming requests and route them through JSONRPCDispatcher.
        """
        try:
            request = conn.recv(1024).decode('utf-8')
            print(f"MasterNode received request: {request}")

            # Dispatch the JSON-RPC request
            response = self.dispatcher.handle_request(request)

            # Send the response back to the client
            conn.sendall(response.encode('utf-8'))
        except Exception as e:
            print(f"Error handling connection: {e}")
            error_response = json.dumps({
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": str(e)},
                "id": None
            })
            conn.sendall(error_response.encode('utf-8'))
        finally:
            conn.close()


    def receive_heartbeat(self, params):
        worker_ip = params["worker_ip"]
        if worker_ip not in self.client_registry and worker_ip in self.inactive_workers:
            self.client_registry[worker_ip] = self.inactive_workers[worker_ip]
            del self.inactive_workers[worker_ip]
        if worker_ip in self.client_registry:
            self.client_registry[worker_ip]["last_heartbeat"] = time.time()
            self.client_registry[worker_ip]["status"] = True
            print(f"Heartbeat received from worker {worker_ip}")

    def monitor_worker_heartbeats(self):
        while True:
            current_time = time.time()
            for worker_ip, info in self.client_registry.items():
                last_heartbeat = info["last_heartbeat"]
                if info["status"] == True and current_time - last_heartbeat > self.heartbeat_timeout:
                    self.client_registry[worker_ip]["status"] = False
                    self.inactive_workers[worker_ip] = self.client_registry[worker_ip]
                    del self.client_registry[worker_ip]
                    print(f"Worker {worker_ip} marked as inactive (no heartbeat)")
            time.sleep(15)  


    def store_data_location(self, conn, original_file_name, client_address, chunked_file_name=None):
        result = self.data_manager_proxy.store_data_location(
            original_file_name=original_file_name,
            client_address=client_address,
            chunked_file_name=chunked_file_name
        )
        response = {"jsonrpc": "2.0", "result": result, "id": 3} if result == "Success" else {"jsonrpc": "2.0", "error": result, "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def get_data_location(self, conn, data_key):
        result = self.data_manager_proxy.get_data_location(original_file_name=data_key)
        response = {"jsonrpc": "2.0", "result": result, "id": 3} if isinstance(result, list) else {"jsonrpc": "2.0", "error": result, "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def send_job(self, job):
        for file in job['files']:
            self.data_manager_proxy.store_data_location(file, (job['client_ip'], job['client_port']))
        self.job_manager_proxy.submit_job(job=job['tasks'])

if __name__ == "__main__":
    master_node = MasterNode()
    # master_node.start()