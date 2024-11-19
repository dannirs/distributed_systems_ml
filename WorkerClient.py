import socket
import threading
from message import message
import json
from handlers import write_to_file
import random
import time
from TaskManager import TaskManager
import struct

class WorkerClient:
    def __init__(self, master_ip, master_port, server_ip, server_port, ip, port):
        self.master_ip = master_ip
        self.master_port = master_port
        self.default_server_ip = server_ip
        self.default_server_port = server_port
        self.ip = ip
        self.port = port
        self.active = True
        self.request_params = None
        self.task_manager = TaskManager(self)
        self.start_listening()

    def send_message(self, s, request_params):
        print("In send_message")
        jsonrpc = "2.0"
        id = random.randint(1, 40000)
        if "payload" not in request_params["params"] or not request_params["params"]["payload"]:
            payload = ""
            request_params["params"]["header_list"]["payload_type"] = 2
        else:
            payload = request_params["params"]["payload"]
            request_params["params"]["header_list"]["payload_type"] = 1

        msg = message(
                        method=request_params["method"], 
                        source_port=s.getsockname(),
                        destination_port=s.getpeername(),
                        header_list=request_params["params"]["header_list"],
                        payload=payload
                    )
        headers = message.process_headers(msg)
        request =   {
                        "jsonrpc": jsonrpc,
                        "method": request_params["method"],
                        "params": headers,
                        "id": id
                    }

        packet = json.dumps(request)
        # packet_bytes = packet.encode('utf-8')
        # message_length = len(packet_bytes)
        # s.sendall(struct.pack('>I', message_length) + packet_bytes)
        s.sendall(packet.encode('utf-8'))

        if request["params"]["payload_type"] == 2:
            payload_json = message.process_payload(msg)
            request =   {
                            "jsonrpc": jsonrpc,
                            "method": request_params["method"],
                            "params": payload_json,
                            "id": id
                        }
            packet = json.dumps(request)
            s.sendall(packet.encode('utf-8'))

        return

    def check_response(self, response_data):
        print(response_data)
        if response_data["result"]["status"] != 200: 
            print("Request failed.")
            return False
        elif response_data["result"]["payload_type"] == 2:
            key = response_data["result"]['key']
            new_key = f'downloaded_{key}'
            write_to_file(response_data["result"]["payload"], new_key)
            print("Request success.")
            print("File retrieved.")
            return True
        elif response_data["result"]["payload_type"] == 1:
            print("Request success.")
            print("Received value: ", response_data["result"]["payload"])
            return True
        else:
            print("Request success.")
            return True

    def send_heartbeat(self):
        while self.active:  # Check if worker is still active
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.connect((self.master_ip, self.master_port))
                    heartbeat_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "heartbeat",
                        "params": {"worker_ip": self.ip, "worker_port": self.port},
                        "id": 1
                    })
                    s.sendall(heartbeat_data.encode('utf-8'))
                    print(f"Heartbeat sent from {self.ip}:{self.port}")
                except ConnectionRefusedError:
                    print("Failed to send heartbeat: Master not reachable")
            time.sleep(10)  # Wait 5 seconds before sending the next heartbeat
        
    def retrieve_data_location(self, task_data, key=None):
        print(self.master_ip)
        print(self.master_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_ip, self.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "get_data_location",
                "params": {"key": key},
                "id": 2
            }
            print(request)
            print(type(request))
            json_request = json.dumps(request).encode('utf-8')
            print(json_request)
            print((type(json_request)))
            print(f"DEBUG: Is socket open? {s.fileno() != -1}")
            s.sendall(json_request)
            response = s.recv(1024).decode('utf-8')
            response_data = json.loads(response)
            # Parse the response
            if "result" in response_data:
                location = response_data["result"]
                print(f"Data '{key}' is located at worker {location}")
                self.connect_to_data_server(location, task_data)
            else:
                print(f"Data '{key}' not found")
                return None

    def start_listening(self):
        
        # Start heartbeat in a separate thread
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()

        # # Start listening for task assignments from the master
        # with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        #     server_socket.bind((self.ip, self.port))
        #     server_socket.listen(5)
        #     print(f"Worker {self.ip}:{self.port} listening for tasks")
        #     while self.active:
        #         conn, addr = server_socket.accept()
        #         # Start a new thread to handle each incoming task request
        #         task_thread = threading.Thread(target=self.handle_task, args=(conn,))
        #         task_thread.start()
    
    def handle_task(self, task_data):
        print(f"Received task: {task_data}")
        self.task_manager.process_task(task_data)

        try:
            if task_data["params"]["method"] == "retrieve_data":
                self.retrieve_data_location(task_data, task_data["params"]["header_list"]["key"])
            else: 
                location = (self.default_server_ip, self.default_server_port)
                self.connect_to_data_server(location, task_data)
        finally:
            # Set active to False to stop the heartbeat thread when main tasks are done
            self.active = False
            print("Worker has stopped; heartbeats will cease.")
            # conn.close()

    # def handle_task(self, conn):
    #     # Receive and process a task from the master
    #     # request = conn.recv(1024).decode('utf-8')
    #     # task_data = json.loads(request)
    #     print(f"Received task: {task_data}")

    #     try:
    #         if task_data["params"]["method"] == "retrieve_data":
    #             self.retrieve_data_location(conn, task_data["params"]["header_list"]["key"])
    #         else: 
    #             self.connect_to_data_server(conn, task_data)
    #         # self.send_message(conn, task_data)
    #         # response = conn.recv(1024).decode('utf-8')
    #         # response_data = json.loads(response)
    #         # task_status = self.check_response(response_data)
    #         # self.task_complete(task_status, task_data["params"]["job_id"], task_data["params"]["task_id"])
    #     finally:
    #         # Set active to False to stop the heartbeat thread when main tasks are done
    #         self.active = False
    #         print("Worker has stopped; heartbeats will cease.")
    #         conn.close()
            
    def connect_to_data_server(self, location, task_data):
        # Connect to the data server at data_location to retrieve or store data
        print("Task Data data server: ", task_data)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((location[0], location[1]))
            self.send_message(s, task_data)
            response = s.recv(1024).decode('utf-8')
            print("Response: ", response)
            response_data = json.loads(response)
            print("Response JSON: ", response_data)
            task_status = self.check_response(response_data)
            s.close() 
            self.task_manager.task_complete(task_status, task_data)

    def test_multiple_clients(self):
        with open('test_input.json', 'r') as file:
            data = json.load(file)
        thread1 = threading.Thread(target=self.start_client, args=(data["test_send_data"],))
        thread2 = threading.Thread(target=self.start_client, args=(data["test_send_value"],))
        thread1.start()
        thread2.start()
        thread1.join() 
        thread2.join() 
        thread3 = threading.Thread(target=self.start_client, args=(data["test_retrieve_data"],))
        thread4 = threading.Thread(target=self.start_client, args=(data["test_retrieve_value"],))
        thread3.start()
        thread4.start()
        thread3.join() 
        thread4.join() 

if __name__ == "__main__":
    client = WorkerClient()
    client.test_multiple_clients()
    # with open('test_input.json', 'r') as file:
    #     data = json.load(file)
    # thread1 = threading.Thread(target=client.start_client, args=(data["test_send_data"],))
    # thread1.start()
    # thread1.join()
    # thread1 = threading.Thread(target=client.start_client, args=(data["test_retrieve_data"],))
    # thread1.start()
    # thread1.join()
    # thread1 = threading.Thread(target=client.start_client, args=(data["test_send_value"],))
    # thread1.start()
    # thread1.join()
    # thread1 = threading.Thread(target=client.start_client, args=(data["test_retrieve_value"],))
    # thread1.start()
    # thread1.join()

