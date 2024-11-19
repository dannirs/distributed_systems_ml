import json 

class JSONRPCProxy:
    def __init__(self, dispatcher, prefix=""):
        self.dispatcher = dispatcher
        self.prefix = prefix

    def __getattr__(self, name):
        """
        Dynamically create a method proxy for JSON-RPC.
        """
        full_method_name = f"{self.prefix}.{name}" if self.prefix else name

        def method_proxy(*args, **kwargs):
            """
            Forward the call as a JSON-RPC request.
            JSON-RPC doesn't support positional arguments, so we convert them to named arguments.
            """
            # If positional arguments are provided, raise an error
            if args:
                raise ValueError("JSON-RPC does not support positional arguments; use keyword arguments instead.")

            # Create and dispatch a JSON-RPC request
            request = json.dumps({
                "jsonrpc": "2.0",
                "method": full_method_name,
                "params": kwargs,
                "id": 1
            })
            response = self.dispatcher.handle_request(request)
            return json.loads(response).get("result")

        return method_proxy
