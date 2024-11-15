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
        else: 
            print("Method not found")
            raise ValueError(f"Unsupported method: {self.method}")

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

    def process_payload(self):
        if self.header_list["payload_type"] == 1:
            raise ValueError("Incorrect payload type")
        if "key" not in self.header_list or not self.header_list["key"]:
            raise ValueError("Missing path to file")
            
        file = self.header_list["key"]
        seq_num = 0
        with open(file, 'rb') as f:
            while True:
                file_data = f.read(1024)
                if not file_data:
                    break
                packet = {
                    "seq_num": seq_num, 
                    "payload": file_data.hex()
                }
                seq_num += 1
        f.close()
        return packet