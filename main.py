import json
from MasterNode import MasterNode
# Simulated JSON-RPC requests
request1 = json.dumps({
    "jsonrpc": "2.0",
    "method": "job.submit_job",
    "params": {"job_data": {"id": 1, "type": "mapreduce"}},
    "id": 1
})

request2 = json.dumps({
    "jsonrpc": "2.0",
    "method": "task.create_task",
    "params": {"task_data": {"id": 101, "description": "Process file"}},
    "id": 2
})

# Call the dispatcher
master = MasterNode(["config1.json", "config4.json"])
response1 = master.dispatcher.handle_request(request1)
response2 = master.dispatcher.handle_request(request2)

print(response1)  # Check for success response
print(response2)
