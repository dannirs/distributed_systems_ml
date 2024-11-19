# utils/jsonrpc_dispatcher.py

import json

class JSONRPCDispatcher:
    def __init__(self):
        """Initialize the dispatcher with an empty registry for methods."""
        self.methods = {}

    def register_method(self, name, func):
        """
        Register a method with the dispatcher.
        :param name: The name of the method (as a string) to be used in JSON-RPC requests.
        :param func: The callable function or method to invoke when this name is called.
        """
        self.methods[name] = func

    def handle_request(self, request):
        try:
            req = json.loads(request)
            method = req.get("method")
            params = req.get("params", {})
            req_id = req.get("id")

            print(f"Received JSON-RPC request: method={method}, params={params}, id={req_id}")

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
