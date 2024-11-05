from MasterNode import MasterNode
import json
import threading
import time

class Node:
    def __init__(self, config):
        self.master = None
        self.config = config
    
    def start_server(self):
        with open(self.config, 'r') as file:
            config = json.load(file)
        for i, dictionary in enumerate(config):
            if config[i]["is_master"]:
                print("Initializing MasterNode")
                ip = config[i]["ip"]
                port = config[i]["port"]
                self.master = MasterNode(config, ip, port)
                self.master.start()
                break

    def send_job(self, job):
        print("In send_job()")
        self.master.send_job(job)

if __name__ == '__main__':
    # A server object is initialized, with a default host and port set. 
    # The server listens for 5 threads at a time. The server remains open
    # even after the connection with a client is terminated.
    # When a client thread is connected, the server goes into the handle_client() method.
    node = Node("config.json")

    # Start the server in a separate thread
    server_thread = threading.Thread(target=node.start_server)
    server_thread.daemon = True  # Daemon thread will close automatically when main program exits
    server_thread.start()
    time.sleep(0.1)
    # Now, you can call send_job without waiting for the server to finish
    node.send_job("job.json")

