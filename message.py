import json

# only include the sequence number if the payload was split 
# add a flag in the header to determine if the payload was split
class message:
    def __init__(self, method, source_port, destination_port, header_list, file=None):
        self.method = method.upper()
        self.headers = {
            "method": self.method,
            "source_port": source_port,
            "destination_port": destination_port
        }
        self.file = file
        self.header_list = header_list
        
    def process_request(self):
        if self.method == "GET_FILE":
            return self.handle_get_file()
        elif self.method == "SEND_FILE":
            return self.handle_send_file()
        elif self.method == "STORE_FILE":
            return self.handle_store_file()
        elif self.method == "RETRIEVE_FILE":
            return self.handle_retrieve_file()
        else:
            raise ValueError(f"Unsupported method: {self.method}")

    def handle_get_file(self):
        if 'file_name_size' in self.header_list and 'file_name' in self.header_list:
            return self.get_file_headers(self.header_list["file_name_size"], self.header_list["file_name"])
        else:
            raise ValueError("Missing parameters for GET_FILE request")

    def handle_send_file(self):
        if 'file_name_size' in self.header_list and 'file_name' in self.header_list and "file_size" in self.header_list:
            headers = self.send_file_headers(self.header_list["file_name_size"], self.header_list["file_name"], self.header_list["file_size"])
            if self.file:
                payload = self.get_payload(self.file)
                return headers, payload
            else:
                raise ValueError("Missing payload for SEND_FILE request")
        else:
            raise ValueError("Missing parameters for SEND_FILE request")

    def handle_store_file(self):
        if 'status_message' in self.header_list:
            return self.store_file_headers(self.header_list["status_message"])
        else:
            raise ValueError("Missing parameters for STORE_FILE response")

    def handle_retrieve_file(self):
        if 'status_message' in self.header_list and self.header_list["status_message"] == "FAILURE":
            return self.retrieve_file_headers(self.header_list["status_message"])
        elif 'status_message' in self.header_list and self.header_list["status_message"] == "SUCCESS" and 'file_size' in self.header_list:
            headers = self.retrieve_file_headers(self.header_list["status_message"], self.header_list["file_size"])
            if self.file:
                payload = self.get_payload(self.file)
                return headers, payload
            else:
                raise ValueError("Missing payload for RETRIEVE_FILE response")
        else:
            raise ValueError("Missing parameters for RETRIEVE_FILE response")

    def get_file_headers(self, file_name_size, file_name):
        packet = {
            "header": {
                **self.headers,
                "file_name_size": file_name_size,
                "file_name": file_name
            }
        }
        return json.dumps(packet, indent=4).encode('utf-8')

    def send_file_headers(self, file_name_size, file_name, file_size):
        packet = {
            "header": {
                **self.headers,
                "file_name_size": file_name_size,
                "file_name": file_name,
                "file_size": file_size
            }
        }
        return json.dumps(packet, indent=4).encode('utf-8')

    def store_file_headers(self, status_message):
        packet = {
            "header": {
                **self.headers,
                "status_message": status_message
            }
        }
        return json.dumps(packet, indent=4).encode('utf-8')

    def retrieve_file_headers(self, status_message, file_size=None):
        packet = {
            "header": {
                **self.headers,
                "status_message": status_message,
                "file_size": file_size
            }
        }
        return json.dumps(packet, indent=4).encode('utf-8')

    def get_payload(self, file):
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
