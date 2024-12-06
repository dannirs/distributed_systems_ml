import os
import json

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

    # Existing methods (req_preprocess_send_data, req_preprocess_retrieve_data, etc.)...

    def req_preprocess_map(self):
        """
        Preprocess headers for Map tasks.
        """
        print("map headers: ", self.header_list)
        if "key" not in self.header_list or not self.header_list["key"]:
            raise ValueError("Missing chunked file parameter for map task")

        self.header_list["payload_type"] = 1 if self.payload else 2
        return self.header_list

    def req_preprocess_reduce(self):
        """
        Preprocess headers for Reduce tasks.
        """
        print(self.header_list)
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

    # def process_payload(self):
    #     """
    #     Process payload for file-based operations, sending the entire payload in one packet.
    #     """
    #     if "key" not in self.header_list or not self.header_list["key"]:
    #         raise ValueError("Missing path to file")
        
    #     file_path = self.header_list["key"]

    #     with open(file_path, 'rb') as file:
    #         # Read the entire file and encode it as a hexadecimal string
    #         payload = file.read().hex()

    #     # Return the entire payload as a single packet
    #     return [{"payload": payload}]

    def check_file_type(self, file_path):
        """
        Check if the given file path is a JSON or CSV file.

        Args:
            file_path (str): The file path to check.

        Returns:
            str: The file type ('json', 'csv', or 'unknown').
        """
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
        """
        Process payload for file-based operations, splitting into chunks for transmission.
        """
        if "key" not in self.header_list or not self.header_list["key"]:
            raise ValueError("Missing path to file")
            
        file_path = self.header_list["key"]
        packets = []
        seq_num = 0
        batch_size = 20
        if self.check_file_type(file_path) == "json":
            with open(file_path, 'r') as file:
                json_data = json.load(file)  # Load entire JSON file

            # Batch the data
            for i in range(0, len(json_data), batch_size):
                batch = json_data[i:i + batch_size]  # Create a batch of items
                payload = json.dumps(batch).encode('utf-8').hex()  # Serialize and encode

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

        else:
            with open(file_path, 'rb') as file:
                payload = ""
                while True:
                    file_data = file.read(102400)  # Read 1024 bytes per packet
                    if not file_data:
                        break
                    packet = {
                        "seq_num": seq_num,
                        "finished": False,
                        "payload": file_data.hex()  # Convert to hex for transport
                    }
                    packets.append(packet)
                    seq_num += 1

        packets[-1]["finished"] = True
        print("# of packets: ", len(packets))
        return packets  # Return all packets as a list


    # def process_payload(self):
    #     """
    #     Process payload for file-based operations.
    #     """
    #     if self.header_list["payload_type"] == 1:
    #         raise ValueError("Incorrect payload type")
    #     if "key" not in self.header_list or not self.header_list["key"]:
    #         raise ValueError("Missing path to file")
            
    #     file = self.header_list["key"]
    #     seq_num = 0
    #     with open(file, 'rb') as f:
    #         while True:
    #             file_data = f.read(1024)
    #             print(file_data)
    #             if not file_data:
    #                 break
    #             packet = {
    #                 "seq_num": seq_num, 
    #                 "payload": file_data.hex()
    #             }
    #             seq_num += 1
    #     f.close()
    #     return packet


# class message:
#     def __init__(self, method, source_port, destination_port, header_list, payload=None):
#         self.method = method
#         self.header_list = header_list
#         self.header_list.update({"method": self.method})
#         self.header_list.update({"source_port": source_port})
#         self.header_list.update({"destination_port": destination_port})
#         self.payload = payload
    
#     def process_headers(self):
#         if self.method == "send_data":
#             return self.req_preprocess_send_data()
#         elif self.method == "retrieve_data":
#             return self.req_preprocess_retrieve_data()
#         elif self.method == "retrieve_data_resp":
#             return self.resp_preprocess_retrieve_data()
#         elif self.method == "send_data_resp": 
#             return self.resp_preprocess_send_data()
#         else: 
#             print("Method not found")
#             raise ValueError(f"Unsupported method: {self.method}")

#     def req_preprocess_send_data(self):
#         if "key" not in self.header_list: 
#             raise ValueError("Missing parameters for request")
#         if not self.header_list["key"]:
#             raise ValueError("Missing parameters for request")
#         self.header_list["status"] = None
#         self.header_list["file_path"] = ""
#         if self.payload: 
#             self.header_list["payload_type"] = 1
#             self.header_list["file_path"] = self.payload
#         else:
#             self.header_list["payload_type"] = 2
#         return self.header_list

#     def req_preprocess_retrieve_data(self):
#         if "key" not in self.header_list: 
#             raise ValueError("Missing parameters for request")
#         if not self.header_list["key"]:
#             raise ValueError("Missing parameters for request")

#         self.header_list["payload_type"] = 0
#         self.header_list["status"] = None
#         return self.header_list
    
#     def resp_preprocess_retrieve_data(self):
#         if "key" not in self.header_list or not self.header_list["key"]: 
#             raise ValueError("Missing parameters for request")
#         if "status" not in self.header_list:
#             raise ValueError("Missing parameters for request")
#         if "payload_type" not in self.header_list or not self.header_list["payload_type"]:
#             raise ValueError("Missing parameters for request")
#         return self.header_list
    
#     def resp_preprocess_send_data(self):
#         if "key" not in self.header_list: 
#             raise ValueError("Missing parameters for request")
#         if not self.header_list["key"]:
#             raise ValueError("Missing parameters for request")
#         if "status" not in self.header_list:
#             raise ValueError("Missing parameters for request")
        
#         self.header_list["payload_type"] = 0
#         return self.header_list

#     def process_payload(self):
#         if self.header_list["payload_type"] == 1:
#             raise ValueError("Incorrect payload type")
#         if "key" not in self.header_list or not self.header_list["key"]:
#             raise ValueError("Missing path to file")
            
#         file = self.header_list["key"]
#         seq_num = 0
#         with open(file, 'rb') as f:
#             while True:
#                 file_data = f.read(1024)
#                 if not file_data:
#                     break
#                 packet = {
#                     "seq_num": seq_num, 
#                     "payload": file_data.hex()
#                 }
#                 seq_num += 1
#         f.close()
#         return packet