import socket
import threading
from message import message
import json
from handlers import write_to_file
import random

class WorkerClient:
    def __init__(self, server_ip, server_port, ip, port):
        # self.server_ip = server_ip
        # self.server_port = server_port
        # self.ip = ip
        # self.port = port

    def send_message(self, s, request_params):
        jsonrpc = "2.0"
        id = random.randint(1, 40000)
        if "payload" not in request_params or not request_params["payload"]:
            payload = ""
            request_params["header_list"]["payload_type"] = 2
        else:
            payload = request_params["payload"]
            request_params["header_list"]["payload_type"] = 1

        msg = message(
                        method=request_params["method"], 
                        source_port=s.getsockname(),
                        destination_port=s.getpeername(),
                        header_list=request_params["header_list"],
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
        else:
            print("Request success.")


    def start_client(self, request_params):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', 0))
            s.connect(('localhost', 5678))
            self.send_message(s, request_params)

            response = s.recv(1024).decode('utf-8')
            response_data = json.loads(response)
            self.check_response(response_data)
        return

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

