# utils/jsonrpc_dispatcher.py

import json

class JSONRPCDispatcher:
    def __init__(self):
        self.methods = {}

    def register_method(self, name, func):
        self.methods[name] = func

    def handle_request(self, request):
        try:
            req = json.loads(request)
            method = req.get("method")
            params = req.get("params", {})
            req_id = req.get("id")

            if not method or method not in self.methods:
                raise ValueError(f"Method '{method}' not found")

            result = self.methods[method](**params)
            response = {
                "jsonrpc": "2.0",
                "result": result,
                "id": req_id,
            }
        except Exception as e:
            response = {
                "jsonrpc": "2.0",
                "error": {"code": -32603, "message": str(e)},
                "id": req_id,
            }
        return json.dumps(response)
