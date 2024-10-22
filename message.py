import json
import os

# only include the sequence number if the payload was split 
# add a flag in the header to determine if the payload was split
class message:
    def __init__(self, packet, source_port, dest_port):
        self.method = packet["method"].lower()
        # self.headers = {
        #     "source_port": source_port,
        #     "destination_port": destination_port
        # }
        self.header_list = packet["params"]["header_list"]
        self.payload = packet["params"]["payload"]
    
    def process_headers(self):
        if self.method == "send_file":
            return self.req_preprocess_send_file()
        elif self.method == "retrieve_file":
            return self.req_preprocess_retrieve_file()
        elif self.method == "retrieve_file_resp":
            return self.resp_preprocess_retrieve_file()
        elif self.method == "send_file_resp": 
            return self.resp_preprocess_send_file()
        else: 
            print("Method not found")
            raise ValueError(f"Unsupported method: {self.method}")

    def req_preprocess_send_file(self):
        if "file_name" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["file_name"]:
            raise ValueError("Missing parameters for request")
        if not self.payload:
            raise ValueError("Missing parameters for request")

        file_size = os.path.getsize(self.header_list["header_list"]["file_name"])
        self.header_list["file_size"] = file_size
        self.header_list["payload_type"] = 2
        self.header_list["status"] = None
        return json.dumps(self.header_list, indent=4).encode('utf-8')
        

    def req_preprocess_retrieve_file(self):
        if "file_name" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["file_name"]:
            raise ValueError("Missing parameters for request")

        file_size = os.path.getsize(self.header_list["header_list"]["file_name"])
        self.header_list["file_size"] = file_size
        self.header_list["payload_type"] = 0
        self.header_list["status"] = None
        return json.dumps(self.header_list, indent=4).encode('utf-8')
    
    def resp_preprocess_retrieve_file(self):
        if "file_name" not in self.header_list or not self.header_list["file_name"]: 
            raise ValueError("Missing parameters for request")
        if "file_path" not in self.header_list or not self.header_list["file_path"]:
            raise ValueError("Missing parameters for request")
        if "file_size" not in self.header_list or not self.header_list["file_size"]:
            raise ValueError("Missing parameters for request")
        if "status" not in self.header_list:
            raise ValueError("Missing parameters for request")

        if self.header_list["status"] != 200:
            self.header_list["need_to_write"] = False
        else:
            file_size = os.path.getsize(self.header_list["header_list"]["file_name"])
            self.header_list["file_size"] = file_size
            self.header_list["payload_type"] = 2
            self.header_list["need_to_write"] = True
        return json.dumps(self.header_list, indent=4).encode('utf-8')
    
    def resp_preprocess_send_file(self):
        if "file_name" not in self.header_list: 
            raise ValueError("Missing parameters for request")
        if not self.header_list["file_name"]:
            raise ValueError("Missing parameters for request")
        if "status" not in self.header_list:
            raise ValueError("Missing parameters for request")
        
        self.header_list["payload_type"] = 0
        self.header_list["need_to_write"] = False
        return json.dumps(self.header_list, indent=4).encode('utf-8')

    def process_payload(self):
        if self.header_list["payload_type"] != 2:
            raise ValueError("Incorrect payload type")
        else:
            if "file_path" not in self.header_list or not self.header_list["file_path"]:
                raise ValueError("Missing path to file")
            
            file = self.header_list["file_path"]
            data_list = []
            seq_num = 0
            with open(file, 'rb') as f:
                while True:
                    file_data = f.read(1024)
                    if not file_data:
                        break
                    packet = {
                        "seq_num": seq_num, 
                        "payload": {
                            "file_data": file_data.hex()
                        }
                    }
                    data_list.append(json.dumps(packet, indent=4).encode('utf-8'))
                    seq_num += 1
            f.close()
            return data_list
        
            # return self.read_file_payload()
        # elif self.header_list["payload_type"] == 1:
        #     if ["key"] not in self.header_list:
        #         raise ValueError("Missing parameters for request")
        #     if not self.payload:
        #         raise ValueError("Missing parameters for request")
        #     packet = {
        #                 "key": self.header_list["key"], 
        #                 "payload": self.payload
        #             }
            # return json.dumps(self.payload, indent=4).encode('utf-8')

    # def read_file_payload(self):
    #     file = self.header_list["file_path"]
    #     data_list = []
    #     seq_num = 0
    #     with open(file, 'rb') as f:
    #         while True:
    #             file_data = f.read(1024)
    #             if not file_data:
    #                 break
    #             packet = {
    #                 "seq_num": seq_num, 
    #                 "payload": {
    #                     "file_data": file_data.hex()
    #                 }
    #             }
    #             data_list.append(json.dumps(packet, indent=4).encode('utf-8'))
    #             seq_num += 1
    #     f.close()
    #     return data_list




    # def process_request(self):
    #     if self.method == "GET_FILE":
    #         return self.handle_get_file()
    #     elif self.method == "SEND_FILE":
    #         return self.handle_send_file()
    #     elif self.method == "STORE_FILE":
    #         return self.handle_store_file()
    #     elif self.method == "RETRIEVE_FILE":
    #         return self.handle_retrieve_file()
    #     else:
    #         raise ValueError(f"Unsupported method: {self.method}")

    # def handle_get_file(self):
    #     if 'file_name_size' in self.header_list and 'file_name' in self.header_list:
    #         return self.get_file_headers(self.header_list["file_name_size"], self.header_list["file_name"])
    #     else:
    #         raise ValueError("Missing parameters for GET_FILE request")

    # def handle_send_file(self):
    #     if 'file_name_size' in self.header_list and 'file_name' in self.header_list and "file_size" in self.header_list:
    #         headers = self.send_file_headers(self.header_list["file_name_size"], self.header_list["file_name"], self.header_list["file_size"])
    #         if self.file:
    #             payload = self.get_payload(self.file)
    #             return headers, payload
    #         else:
    #             raise ValueError("Missing payload for SEND_FILE request")
    #     else:
    #         raise ValueError("Missing parameters for SEND_FILE request")

    # def handle_store_file(self):
    #     if 'status_message' in self.header_list:
    #         return self.store_file_headers(self.header_list["status_message"])
    #     else:
    #         raise ValueError("Missing parameters for STORE_FILE response")

    # def handle_retrieve_file(self):
    #     if 'status_message' in self.header_list and self.header_list["status_message"] == "FAILURE":
    #         return self.retrieve_file_headers(self.header_list["status_message"])
    #     elif 'status_message' in self.header_list and self.header_list["status_message"] == "SUCCESS" and 'file_size' in self.header_list:
    #         headers = self.retrieve_file_headers(self.header_list["status_message"], self.header_list["file_size"])
    #         if self.file:
    #             payload = self.get_payload(self.file)
    #             return headers, payload
    #         else:
    #             raise ValueError("Missing payload for RETRIEVE_FILE response")
    #     else:
    #         raise ValueError("Missing parameters for RETRIEVE_FILE response")

    # def get_file_headers(self, file_name_size, file_name):
    #     packet = {
    #         "header": {
    #             **self.headers,
    #             "file_name_size": file_name_size,
    #             "file_name": file_name
    #         }
    #     }
    #     return json.dumps(packet, indent=4).encode('utf-8')

    # def send_file_headers(self, file_name_size, file_name, file_size):
    #     packet = {
    #         "header": {
    #             **self.headers,
    #             "file_name_size": file_name_size,
    #             "file_name": file_name,
    #             "file_size": file_size
    #         }
    #     }
    #     return json.dumps(packet, indent=4).encode('utf-8')

    # def store_file_headers(self, status_message):
    #     packet = {
    #         "header": {
    #             **self.headers,
    #             "status_message": status_message
    #         }
    #     }
    #     return json.dumps(packet, indent=4).encode('utf-8')

    # def retrieve_file_headers(self, status_message, file_size=None):
    #     packet = {
    #         "header": {
    #             **self.headers,
    #             "status_message": status_message,
    #             "file_size": file_size
    #         }
    #     }
    #     return json.dumps(packet, indent=4).encode('utf-8')
