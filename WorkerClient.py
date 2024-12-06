import socket
import threading
from message import message
import json
from handlers import write_to_file
import random
import time
from TaskManager import TaskManager
from JSONRPCProxy import JSONRPCProxy
import os
from main import write_packets_to_file, read_packets_from_file, verify_format 

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

        # print("Request params: ", request_params)
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

        print("sending header request: ", request)
        packet = json.dumps(request)
        s.sendall(packet.encode('utf-8'))
        time.sleep(1)  
        if request_params["method"] == "reduce": 
            # request =   {
            #                 "jsonrpc": jsonrpc,
            #                 "method": request_params["method"],
            #                 "params": {"finished": request_params["params"]["header_list"]["finished"], "payload": request_params["params"]["payload"]},
            #                 "id": id
            #             }
            # packet = json.dumps(request)
            # s.sendall(packet.encode('utf-8'))
            packets = message.process_payload(msg)


            write_packets_to_file(packets, "packets.json")

            # Step 3: Read packets from the JSON file and verify structure
            reconstructed_data = read_packets_from_file("packets.json")

            # Step 4: Verify format
            format_is_correct = verify_format(reconstructed_data)

            print("\nDEBUG: Reconstructed data:", reconstructed_data)
            print("\nDEBUG: Format is correct:", format_is_correct)


            for packet in packets:
                packet["finished"] = request_params["params"]["header_list"]["finished"]
                payload_request = {
                    "jsonrpc": jsonrpc,
                    "method": request_params["method"],
                    "params": packet,
                    "id": id
                }
                s.sendall(json.dumps(payload_request).encode('utf-8'))
            print("sending payload")
            # print(f"Sending packet: {json.dumps(payload_request)}")
        elif request["params"]["header_list"]["payload_type"] == 2:
            packets = message.process_payload(msg)
            print("payload packets: ", packets)
            for i, packet in enumerate(packets):
                is_last_packet = (i == len(packets) - 1)
                # packet["finished"] = is_last_packet
                payload_request = {
                    "jsonrpc": jsonrpc,
                    "method": request_params["method"],
                    "params": packet,
                    "id": id
                }
                # print(payload_request)
                try: 
                    serialized_packet = json.dumps(payload_request)
                    print("Sending payload packet:", serialized_packet)
                    s.sendall(serialized_packet.encode('utf-8'))
                except Exception as e:
                    print(f"Error serializing payload packet: {e}")
                    return



            # payload_json = message.process_payload(msg)
            # request =   {
            #                 "jsonrpc": jsonrpc,
            #                 "method": request_params["method"],
            #                 "params": payload_json,
            #                 "id": id
            #             }
            # packet = json.dumps(request)
            # s.sendall(packet.encode('utf-8'))

        return

    def check_response(self, response_data):
        # print(response_data)
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
        while self.active:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
                    heartbeat_data = json.dumps({
                        "jsonrpc": "2.0",
                        "method": "heartbeat",
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

    def retrieve_data_location(self, task_data, key=None):
        print(self.master_ip)
        print(self.master_port)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_ip, self.master_port))
            request = {
                "jsonrpc": "2.0",
                "method": "data.get_data_location",
                "params": {"original_file_name": key},
                "id": 2
            }
            # print(request)
            # print(type(request))
            json_request = json.dumps(request).encode('utf-8')
            # print(json_request)
            # print((type(json_request)))
            print(f"DEBUG: Is socket open? {s.fileno() != -1}")
            s.sendall(json_request)
            response = s.recv(1024).decode('utf-8')
            response_data = json.loads(response)
            if "result" in response_data:
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
        print(f"Received task: {task_data}")
        self.task_manager_proxy.process_task(task=task_data)

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
            print("Worker has stopped; heartbeats will cease.")

    def connect_to_data_server(self, location, task_data):
        print("Task Data data server: ", task_data)
        print("location: ", location)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((location[0], location[1]))
            self.send_message(s, task_data)
            response = s.recv(1024).decode('utf-8')
            # while response:            
            print("Response: ", response)
            response_data = json.loads(response)
            print("Response JSON: ", response_data)
            task_status = self.check_response(response_data)
            s.close() 
            self.task_manager_proxy.task_complete(task_status=task_status, task_data=task_data)

    def handle_reduce_task(self, task_data):
        """
        Process a Reduce task by retrieving map results and sending them to the Reduce Server.

        Args:
            task_data (dict): Task data for the Reduce phase.
        """
        print("Task data for Reduce:", task_data)
        map_result_files = task_data["params"]["header_list"]["map_results"]  # List of files to process
        reduce_server_location = (self.default_server_ip, self.default_server_port)  # Reduce server location

        # Step 1: Retrieve map result locations from DataManager (via MasterNode)
        map_result_locations = []
        for map_result_file in map_result_files:
            player_result_path = f"map_result_player_{os.path.basename(map_result_file)}.json"
            team_result_path = f"map_result_team_{os.path.basename(map_result_file)}.json"
            map_locations = self.retrieve_map_results(player_result_path)  # Retrieve from DataManager
            map_result_locations.extend(map_locations)
            # map_locations = self.retrieve_map_results(team_result_path)  # Retrieve from DataManager
            # map_result_locations.extend(map_locations)


        print(f"Map result locations: {map_result_locations}")

        # Step 2: Collect map results from the retrieved locations
        collected_map_results = self.collect_map_results(map_result_locations)
        print("collected map results: ", collected_map_results)

        print(f"Collected Map results: {len(collected_map_results)} files")

        # Step 3: Send the collected Map results to the Reduce WorkerServer
        self.send_results_to_reduce_server(reduce_server_location, collected_map_results)
        print("Reduce task has been successfully executed.")


    def collect_map_results(self, map_result_locations):
        """
        Connect to WorkerServers to retrieve all Map results.

        Args:
            map_result_locations (list): A list of dictionaries containing file_name and location
                                        (e.g., [{"file_name": "file1map_1", "location": ("worker_ip", 8080)}])

        Returns:
            list: Collected Map results.
        """
        collected_results = []
        print("map locations: ", map_result_locations)
        for entry in map_result_locations:
            print("location: ", entry)
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
                print("Response: ", response)
                response_data = json.loads(response)
                if "result" in response_data and response_data["result"]:
                    collected_results.append(response_data["result"])
                    write_to_file(response_data["result"]["payload"], "jdalkfj.json")
                    print(f"Successfully retrieved Map result for {file_name}")
                else:
                    print(f"Error retrieving Map result for {file_name}: {response_data.get('error', 'Unknown error')}")

        return collected_results

                    



        #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        #         try:
        #             # Connect to the WorkerServer
        #             s.connect(tuple(location))

        #             # Send a retrieve_data request
        #             request = {
        #                 "jsonrpc": "2.0",
        #                 "method": "retrieve_data",
        #                 "params": {"key": file_name},
        #                 "id": 1
        #             }
        #             s.sendall(json.dumps(request).encode('utf-8'))

        #             # Receive and process the response
        #             response = json.loads(s.recv(4096).decode('utf-8'))
        #             if "result" in response and response["result"]:
        #                 collected_results.append(response["result"])
        #                 print(f"Successfully retrieved Map result for {file_name}")
        #             else:
        #                 print(f"Error retrieving Map result for {file_name}: {response.get('error', 'Unknown error')}")

        #         except Exception as e:
        #             print(f"Error connecting to {location} for file {file_name}: {e}")

        # return collected_results



    def retrieve_map_results(self, original_file_name):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.master_ip, self.master_port))
            request = {"jsonrpc": "2.0", "method": "data.get_data_location", "params": {"original_file_name": original_file_name}, "id": 1}
            s.sendall(json.dumps(request).encode('utf-8'))
            response = json.loads(s.recv(4096).decode('utf-8'))
            print("Got response: ", response)
            return response.get("result", [])

    def send_results_to_reduce_server(self, reduce_server_location, collected_map_results):
        """
        Sends all collected Map results to the specified Reduce server.

        Args:
            reduce_server_location (tuple): A tuple containing the Reduce server's (IP, port).
            collected_map_results (list): List of collected Map results.
        """
        print(f"Sending collected Map results to Reduce server at {reduce_server_location}")


        #         # Send task_data to the server or handle it as needed
        #         # (e.g., send via JSON-RPC or add to a queue)
                
        #     except Exception as e:
        #         print(f"Error processing map result: {e}")



        # for map_result in collected_map_results:
        #     try:
        #         task_data = {
        #             "jsonrpc": "2.0",
        #             "method": "reduce",  # Assuming the reduce server expects this method
        #             "params": {
        #                 "header_list": {
        #                     "key": map_result['key'],  # Use the key from the map result
        #                     "finished": False
        #                 },
        #                 "payload": map_result['payload']  # Include the payload data
        #             }, 
        #             "id": random.randint(1, 10000)
        #         }
                # task_data = {
                #     "method": "reduce",
                #     "header_list": {
                #         "key": map_result['key']
                #     },
                #     "payload": map_result['payload']
                # }
                # Create a socket connection to the reduce server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((reduce_server_location[0], reduce_server_location[1]))
            for i, map_result in enumerate(collected_map_results):
                print("sending packet: ", i)
                # try:
                # Determine if this is the last packet
                is_last_packet = (i == len(collected_map_results) - 1)

                task_data = {
                    "jsonrpc": "2.0",
                    "method": "reduce",  # Assuming the reduce server expects this method
                    "params": {
                        "header_list": {
                            "key": map_result['key'],  # Use the key from the map result
                            "finished": is_last_packet  # Set to True if this is the last packet
                        },
                        "payload": map_result['payload']  # Include the payload data
                    },
                    "id": random.randint(1, 10000)
                }
            
            # Send the request using send_message for proper JSON-RPC formatting
                self.send_message(s, task_data)

            # Receive and process the response
            # response = s.recv(4096).decode('utf-8')
            # print("Response from Reduce server: ", response)
            # response_data = json.loads(response)

            # if response_data.get("result") == "success":
            #     print(f"Successfully sent Map result for key {map_result['key']}")
            # else:
            #     print(f"Failed to send Map result for key {map_result['key']}: {response_data.get('error', 'Unknown error')}")
    # except Exception as e:
    #     print(f"Error connecting to Reduce server at {reduce_server_location}: {e}")



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

