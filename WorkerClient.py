import socket
import threading
from message import message
import json
from handlers import write_to_file, write_to_file_csv, log_data_summary
import random
import time
from TaskManager import TaskManager
from JSONRPCProxy import JSONRPCProxy
import os

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
        self.task_manager_proxy = JSONRPCProxy(self.task_manager.dispatcher, prefix="task")

        self.start_listening()

    def send_message(self, s, request_params):
        time.sleep(1) 
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
                        "params": {"header_list":headers},
                        "id": id
                    }

        packet = json.dumps(request)
        s.sendall(packet.encode('utf-8'))
        time.sleep(1)  
        if request_params["method"] == "reduce": 
            packets = message.process_payload(msg)

            for packet in packets:
                packet["finished"] = request_params["params"]["header_list"]["finished"]
                payload_request = {
                    "jsonrpc": jsonrpc,
                    "method": request_params["method"],
                    "params": packet,
                    "id": id
                }
                s.sendall(json.dumps(payload_request).encode('utf-8'))
        elif request["params"]["header_list"]["payload_type"] == 2:
            packets = message.process_payload(msg)
            for i, packet in enumerate(packets):
                if isinstance(packet["payload"], str):
                    try:
                        json.loads(packet["payload"])
                    except json.JSONDecodeError:
                        pass  # It's not JSON-encoded, so leave as-is

                payload_request = {
                    "jsonrpc": jsonrpc,
                    "method": request_params["method"],
                    "params": packet,
                    "id": id
                }
                try: 
                    serialized_packet = json.dumps(payload_request)
                    s.sendall(serialized_packet.encode('utf-8'))
                except Exception as e:
                    print(f"Error serializing payload packet: {e}")
                    return
        return

    def check_response(self, response_data):
        if response_data["result"]["status"] != 200: 
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
        while self.active:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
                    heartbeat_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "master.receive_heartbeat",
                        "params": {
                            "worker_ip": self.ip,
                            "worker_port": self.port
                        },
                        "id": 1
                    })
                    udp_socket.sendto(heartbeat_data.encode('utf-8'), (self.master_ip, self.master_port))
                    print(f"Heartbeat sent via UDP from {self.ip}:{self.port}")
            except Exception as e:
                print(f"Failed to send heartbeat via UDP: {e}")

            time.sleep(10)  

    def retrieve_data_location(self, task_data, key=None, method=None):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_ip, self.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "data.get_data_location",
                "params": {"original_file_name": key},
                "id": 2
            }
            json_request = json.dumps(request).encode('utf-8')
            print(f"DEBUG: Is socket open? {s.fileno() != -1}")
            s.sendall(json_request)
            response = s.recv(1024).decode('utf-8')
            response_data = json.loads(response)
            print("task_data: ", task_data)
            if "result" in response_data and method == "data.get_data_location":
                task = {"jsonrpc": "2.0",
                "method": "retrieve_data",
                "params": {
                    "header_list": {
                        "key": key
                    },
                    "payload": ""
                },
                "id": random.randint(1, 10000)}
                location = response_data["result"]   
                print(f"Data '{key}' is located at worker {location}")
                self.connect_to_data_server_chunk_file(location[0][1], task)
            elif "result" in response_data:
                location = response_data["result"]
                print(f"Data '{key}' is located at worker {location}")
                self.connect_to_data_server(location[0][1], task_data)
            else:
                print(f"Data '{key}' not found")
                return None

    def start_listening(self):
        heartbeat_thread = threading.Thread(target=self.send_heartbeat, daemon=True)
        heartbeat_thread.start()
    
    def handle_task(self, task_data):
        self.task_manager_proxy.process_task(task=task_data)
        if task_data["params"]["method"] == "map":
            file_name = task_data["params"]["header_list"]["key"]
            if not os.path.isfile(file_name):
                task =     {
                                "method": "data.get_data_location",
                                "header_list": {
                                    "key": file_name
                                },
                                "payload": ""
                            }
                self.retrieve_data_location(task, file_name, "data.get_data_location")
        try:
            if task_data["params"]["method"] == "retrieve_data":
                self.retrieve_data_location(task_data, task_data["params"]["header_list"]["key"])
            elif task_data["params"]["method"] == "reduce":
                self.handle_reduce_task(task_data)
            else: 
                location = (self.default_server_ip, self.default_server_port)
                self.connect_to_data_server(location, task_data)
        finally:
            self.active = False
            print("WorkerClient has stopped; heartbeats will cease.")

    def connect_to_data_server_chunk_file(self, location, task_data):
        all_data = []
        buffer = ""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((location[0], location[1]))
            self.send_message(s, task_data)
            while True:
                chunk = s.recv(1000000).decode('utf-8')
                if not chunk:
                    break
                buffer += chunk
                while "\n" in buffer:
                    payload, buffer = buffer.split("\n", 1)  # Split buffer at the first newline
                    if payload.strip():
                        try:
                            parsed_payload = json.loads(payload)  # Parse JSON payload
                            if isinstance(parsed_payload, list):
                                all_data.extend(parsed_payload)  # Add rows to the all_data list
                            else:
                                print("Skipping invalid payload (not a list):", parsed_payload)  # Add rows to all_data
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON: {e}")

            if all_data:
                write_to_file_csv(task_data["params"]["header_list"]["key"], all_data)
            s.close() 
        self.task_manager_proxy.task_complete(task_status=200, task_data=task_data)

    def connect_to_data_server(self, location, task_data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((location[0], location[1]))
            self.send_message(s, task_data)
            response = s.recv(1000000).decode('utf-8')
            response_data = json.loads(response)
            task_status = self.check_response(response_data)
            s.close() 
            self.task_manager_proxy.task_complete(task_status=task_status, task_data=task_data)

    def handle_reduce_task(self, task_data):
        map_result_files = task_data["params"]["header_list"]["map_results"]  # List of files to process
        reduce_server_location = (self.default_server_ip, self.default_server_port)  # Reduce server location

        # Step 1: Retrieve map result locations from DataManager (via MasterNode)
        map_result_locations = []
        for map_result_file in map_result_files:
            player_result_path = f"map_output_{os.path.basename(map_result_file)}.json"
            map_locations = self.retrieve_map_results(player_result_path)  # Retrieve from DataManager
            map_result_locations.extend(map_locations)

        print(f"Map result locations: {map_result_locations}")

        # Step 2: Collect map results from the retrieved locations
        collected_map_results = self.collect_map_results(map_result_locations)
        print(f"Collected Map results: {len(collected_map_results)} files")

        # Step 3: Send the collected Map results to the Reduce WorkerServer
        self.send_results_to_reduce_server(reduce_server_location, collected_map_results)
        print("Reduce task has been successfully executed.")


    def collect_map_results(self, map_result_locations):
        collected_results = []
        for entry in map_result_locations:
            file_name = entry[0]
            location = entry[1]
            print(f"Fetching Map result from {location} for file {file_name}")
            task_data = {
                "jsonrpc": "2.0",
                "method": "retrieve_data",
                "params": {
                    "header_list": {
                        "key": file_name
                    },
                    "payload": ""
                },
                "id": random.randint(1, 10000)
            }

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((location[0], location[1]))
                self.send_message(s, task_data)  # Ensure consistent JSON-RPC call
                response = s.recv(102400).decode('utf-8')
                response_data = json.loads(response)
                if "result" in response_data and response_data["result"]:
                    collected_results.append(response_data["result"])
                    print(f"Successfully retrieved Map result for {file_name}")
                else:
                    print(f"Error retrieving Map result for {file_name}: {response_data.get('error', 'Unknown error')}")

        return collected_results

    def retrieve_map_results(self, original_file_name):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_ip, self.master_port))
            request = {"jsonrpc": "2.0", "method": "data.get_data_location", "params": {"original_file_name": original_file_name}, "id": 1}
            s.sendall(json.dumps(request).encode('utf-8'))
            response = json.loads(s.recv(4096).decode('utf-8'))
            return response.get("result", [])

    def send_results_to_reduce_server(self, reduce_server_location, collected_map_results):
        print(f"Sending collected Map results to Reduce server at {reduce_server_location}")
        print(collected_map_results)
        import base64

        def is_base64(s):
            try:
                return base64.b64encode(base64.b64decode(s)).decode('utf-8') == s
            except Exception:
                return False

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((reduce_server_location[0], reduce_server_location[1]))
            for i, map_result in enumerate(collected_map_results):
                # print("map_result: ", map_result)
                is_last_packet = (i == len(collected_map_results) - 1)
                    # Use payload as-is if already encoded; otherwise, encode it
                # encoded_payload = map_result['payload']
                # if not is_base64(encoded_payload):
                #     encoded_payload = base64.b64encode(encoded_payload.encode('utf-8')).decode('utf-8')
                task_data = {
                    "jsonrpc": "2.0",
                    "method": "reduce",  # Assuming the reduce server expects this method
                    "params": {
                        "header_list": {
                            "key": map_result['key'],  # Use the key from the map result
                            "finished": is_last_packet  # Set to True if this is the last packet
                        },
                        "payload": map_result["payload"]  # Include the payload data
                    },
                    "id": random.randint(1, 10000)
                }
                # if is_last_packet:
                #     s.sendall((json.dumps(task_data)).encode('utf-8'))
            # Send the request using send_message for proper JSON-RPC formatting
                # print("Sending JSON:", json.dumps(task_data))
                # else:
                s.sendall((json.dumps(task_data) + "\n").encode('utf-8'))
                # self.send_message(s, task_data)

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

