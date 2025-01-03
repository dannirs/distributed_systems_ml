from WorkerServer import WorkerServer
from WorkerClient import WorkerClient
import argparse
import json
import os
import socket

def load_config(config_path=None):
    if not config_path:
        config_path = os.path.join(os.path.dirname(__file__), "config.json")
    
    with open(config_path, "r") as f:
        return json.load(f)

def parse_args():
    parser = argparse.ArgumentParser(description="WorkerServer")
    parser.add_argument("--master-ip", required=True, help="MasterNode IP address")
    parser.add_argument("--master-port", required=True, type=int, help="MasterNode port")
    return parser.parse_args()

class Worker:
    def __init__(self, master_ip, master_port, config):
        for i, dictionary in enumerate(config):
            if config[i]["role"] == "WorkerServer":
                self.ip = config[i]["ip"]
                self.port = config[i]["port"]
                break
        self.master_ip = master_ip
        self.master_port = master_port
        self.clients = {}
        self.server = WorkerServer(self.master_ip, self.master_port, self.ip, self.port, self)
        for i, dictionary in enumerate(config):
            if config[i]["role"] == "WorkerClient":
                ip = config[i]["ip"]
                port = config[i]["port"]
                client = WorkerClient(self.master_ip, self.master_port, self.ip, self.port, ip, port)
                self.clients[(ip, port)] = client
    
    def get_clients(self):
        return ((self.ip, self.port), list(self.clients.keys()))

    def start(self):
        if self.server:
            print("Starting Worker node\n")
            self.server.start_server(self.get_clients())
        return

    def check_available_clients(self):
        num_available = 0
        for i, dictionary in enumerate(self.clients):
            if self.clients[i].active == False:
                num_available += 1
        return num_available
    
    def get_client(self, addr):
        if addr in self.clients:
            return self.clients[addr]
        else:
            print("Client not found")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-ip", required=True, help="MasterNode IP Address")
    parser.add_argument("--master-port", required=True, type=int, help="MasterNode Port")
    parser.add_argument('--config', required=True, help="Path to configuration file")
    
    args = parser.parse_args()
    config = load_config(args.config)

    worker = Worker(
        master_ip=args.master_ip,
        master_port=args.master_port, 
        config = config
    )
    worker.start()