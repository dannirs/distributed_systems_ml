from MasterNode import MasterNode

class node:
    def __init__(self, config):
        self.is_master = config["master"]
        self.master = None
        self.ip = None
        self.port = None

        if self.is_master:
            print("Initializing MasterNode")
            self.ip = config["master"]["ip"]
            self.port = config["master"]["port"]
            self.master = MasterNode(self.ip, self.port)
            self.master.start()
    
    def receive_job(self, job):
        self.master.send_job(job)

