from WorkerServer import WorkerServer
from WorkerClient import WorkerClient

class Worker:
    def __init__(self, master_ip, master_port, server_ip, server_port, config):
        self.ip = server_ip
        self.port = server_port
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
            print("Starting server")
            self.server.start_server()
        print("Complete")
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


