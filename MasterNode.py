import socket
import threading
import json
from JobManager import JobManager
import time
from DataManager import DataManager
from JSONRPCProxy import JSONRPCProxy
from JSONRPCDispatcher import JSONRPCDispatcher

class MasterNode:
    def __init__(self, ip="localhost", port=3676):
        self.ip = ip
        self.port = port
        print(f"MasterNode starting on IP: {self.ip} Port: {self.port}")
        
        self.server_registry = {}       
        self.client_registry = {}
        self.client_to_server_registry = {}
        self.job_manager = JobManager(self)  
        self.heartbeat_timeout = 15
        self.inactive_workers = {}
        self.data_manager = DataManager()
        self.dispatcher = JSONRPCDispatcher()

        self.dispatcher.register_method("job.submit_job", self.job_manager.submit_job)
        self.dispatcher.register_method("job.get_task_response", self.job_manager.get_task_response)
        self.dispatcher.register_method("job.get_available_clients", self.job_manager.get_available_clients)
        self.dispatcher.register_method("job.send_task_to_client", self.job_manager.send_task_to_client)
        self.dispatcher.register_method("job.assign_tasks", self.job_manager.assign_tasks)
        self.dispatcher.register_method("data.get_data_location", self.data_manager.get_data_location)
        self.dispatcher.register_method("data.store_data_location", self.data_manager.store_data_location)
        self.dispatcher.register_method("data.store_data_location_client", self.data_manager.store_data_location_client)
        self.dispatcher.register_method("master.receive_heartbeat", self.receive_heartbeat)
        self.dispatcher.register_method("master.receive_node_request", self.receive_node_request)
        self.dispatcher.register_method("master.send_job", self.send_job)

        # Wrap managers with JSONRPCProxy
        self.job_manager_proxy = JSONRPCProxy(self.dispatcher, prefix="job")
        self.data_manager_proxy = JSONRPCProxy(self.dispatcher, prefix="data")

    def receive_node_request(self, ip, port, clients):
        self.server_registry[(ip, port)] = {"status": True}

        self.client_to_server_registry[(ip, port)] = clients

        for client in clients:
            self.client_registry[(client[0], client[1])] = {
                "status": True,
                "task_status": False
            }

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.ip, self.port))
        server_socket.listen(5)
        while True:  
            conn, addr = server_socket.accept()
            print(f"In server, Connected to {addr}")
            try:
                client_thread = threading.Thread(target=self.handle_connection, args=(conn,))
                client_thread.start()
            except Exception as e:
                print(f"Error starting thread for {addr}: {e}")

    def handle_connection(self, conn):
        try:
            request = conn.recv(10000).decode('utf-8')
            print(f"MasterNode received request: {request}")
            response = self.dispatcher.handle_request(request)
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

    def store_data_location_client(self, file, client_address):
        self.data_manager_proxy.store_data_location_client(file=file, client_address=client_address)

    def get_data_location(self, conn, data_key):
        result = self.data_manager_proxy.get_data_location(original_file_name=data_key)
        response = {"jsonrpc": "2.0", "result": result, "id": 3} if isinstance(result, list) else {"jsonrpc": "2.0", "error": result, "id": 3}
        conn.sendall(json.dumps(response).encode('utf-8'))

    def send_job(self, tasks):
        self.job_manager_proxy.submit_job(tasks=tasks)

if __name__ == "__main__":
    master_node = MasterNode()
    master_node.start()