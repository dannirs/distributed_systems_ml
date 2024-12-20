import os
import json
import base64
import csv

class message:
    def __init__(self, method, source_port, destination_port, header_list, payload=None):
        self.method = method
        self.header_list = header_list
        self.header_list.update({"method": self.method})
        self.header_list.update({"source_port": source_port})
        self.header_list.update({"destination_port": destination_port})
        self.payload = payload
    
    def process_headers(self):
        if self.method == "send_data":
            return self.req_preprocess_send_data()
        elif self.method == "retrieve_data":
            return self.req_preprocess_retrieve_data()
        elif self.method == "retrieve_data_resp":
            return self.resp_preprocess_retrieve_data()
        elif self.method == "send_data_resp": 
            return self.resp_preprocess_send_data()
        elif self.method == "map":
            return self.req_preprocess_map()
        elif self.method == "reduce":
            return self.req_preprocess_reduce()
        else: 
            print("Method not found")
            raise ValueError(f"Unsupported method: {self.method}")

    def req_preprocess_map(self):
        if "key" not in self.header_list or not self.header_list["key"]:
            raise ValueError("Missing chunked file parameter for map task")

        self.header_list["payload_type"] = 1 if self.payload else 2
        return self.header_list

    def req_preprocess_reduce(self):
        if "key" not in self.header_list:
            raise ValueError("Missing or invalid map results parameter for reduce task")

        self.header_list["payload_type"] = 0
        return self.header_list

    def req_preprocess_send_data(self):
        if "key" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["key"]:
            raise ValueError("Missing parameters for request")
        self.header_list["status"] = None
        self.header_list["file_path"] = ""
        if self.payload: 
            self.header_list["payload_type"] = 1
            self.header_list["file_path"] = self.payload
        else:
            self.header_list["payload_type"] = 2
        return self.header_list

    def req_preprocess_retrieve_data(self):
        if "key" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["key"]:
            raise ValueError("Missing parameters for request")

        self.header_list["payload_type"] = 0
        self.header_list["status"] = None
        return self.header_list
    
    def resp_preprocess_retrieve_data(self):
        if "key" not in self.header_list or not self.header_list["key"]: 
            raise ValueError("Missing parameters for request")
        if "status" not in self.header_list:
            raise ValueError("Missing parameters for request")
        if "payload_type" not in self.header_list or not self.header_list["payload_type"]:
            raise ValueError("Missing parameters for request")
        return self.header_list
    
    def resp_preprocess_send_data(self):
        if "key" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["key"]:
            raise ValueError("Missing parameters for request")
        if "status" not in self.header_list:
            raise ValueError("Missing parameters for request")
        
        self.header_list["payload_type"] = 0
        return self.header_list

    def check_file_type(self, file_path):
        # Get the file extension
        _, file_extension = os.path.splitext(file_path)
        
        # Check the file type
        if file_extension.lower() == ".json":
            return "json"
        elif file_extension.lower() == ".csv":
            return "csv"
        else:
            return "unknown"


    def process_payload(self):
        if "key" not in self.header_list or not self.header_list["key"]:
            raise ValueError("Missing path to file")
        
        file_path = self.header_list["key"]
        packets = []
        seq_num = 0
        batch_size = 5000
        if self.check_file_type(file_path) == "json":
            with open(file_path, 'r') as file:
                json_data = json.load(file)  
                if isinstance(json_data, dict):
                    raise ValueError("Is not a list.")  
                elif not isinstance(json_data, list):
                    raise ValueError("JSON file does not contain a list or processable data.")

            # Batch the data
            for i in range(0, len(json_data), batch_size):
                batch = json_data[i:i + batch_size]  # Create a batch of items
                payload = json.dumps(batch).encode('utf-8')
                payload=base64.b64encode(payload).decode('utf-8')  

                packet = {
                    "seq_num": seq_num,
                    "finished": False,
                    "payload": payload
                }
                packets.append(packet)
                seq_num += 1

            # Mark the last packet as finished
            if packets:
                packets[-1]["finished"] = True
            return packets

        elif self.check_file_type(file_path) == "csv":
            packets = []
            seq_num = 0

            # Check if the file is CSV
            with open(file_path, 'r', encoding="utf-8", newline='') as file:
                csv_reader = csv.DictReader(file)  # Parse CSV into dictionaries
                csv_data = [row for row in csv_reader]  # Convert to list of dicts

            # Batch the data
            for i in range(0, len(csv_data), batch_size):
                batch = csv_data[i:i + batch_size] 
                
                payload = json.dumps(batch, ensure_ascii=False) 
                try:
                    payload_json = json.loads(payload)  
                except json.JSONDecodeError as e:
                    print("Invalid JSON in message: ", e)
                    continue
                payload_bytes = payload.encode('utf-8')  
                payload_hex = payload_bytes  
                payload_hex = base64.b64encode(payload_bytes).decode('utf-8')  

                packet = {
                    "seq_num": seq_num,
                    "finished": False,
                    "payload": payload_hex  # Keep the payload as a JSON string
                }
                packets.append(packet)
                seq_num += 1

            # Mark the last packet as finished
            if packets:
                packets[-1]["finished"] = True

            return packets

        else:
            with open(file_path, 'rb') as file:
                payload = ""
                while True:
                    file_data = file.read(102400)  
                    if not file_data:
                        break
                    packet = {
                        "seq_num": seq_num,
                        "finished": False,
                        "payload": base64.b64encode(file_data).decode('utf-8')  
                    }
                    packets.append(packet)
                    seq_num += 1

        packets[-1]["finished"] = True
        return packets  # Return all packets as a list