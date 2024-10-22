# distributed_systems_ml

Client: 
-main(): calls test
-test(): contains various input parameters (this is already in a dict), calls send_request
-send_request(): calls prepare_message and send_message
-get_payload(): if the message has a payload, create it here and attach it to the parameters
-send_message(): passes the message to the rpc. waits for a response and checks the ack flag to see if the response succeeded; checks the action_required flag to see if further action is needed (write_to_file)
-write_to_file(): called if the response requires the client to write a file to storage
-method name is both separate and in the header list

RPC:
-call(): Uses the client's socket address to create a connection to the server. Parses the method, headers, and payload from the dictionary and creates a Message object. checks the method and, depending on the method, may do additional processing to get more headers (ex. file size).  If there is a problem with the message, it returns an error to the client and closes the connection. Calls send()
-send(): sends the request to the server and waits for a response. passes the response to the client? 
-if the request did not succeed, does the client call the rpc again and the connection is closed and reopened?
-sends headers and payload separately, using 2 sends?

Message:
-does error handling to make sure all parameters are included in the header
-headers are stored in a dictionary 
-payload is separate from the headers

Server: 
-start_server(): connection is initiated
-handle_client(): checks the method to decide which sub-function to call
-file_upload(): stores file. sends ack response
-file_retrieve(): retrieves file from dict. sends file back in payload to the rpc
-value_upload(): stores value. sends ack response
-value_retrieve(): retrieves value from dict. sends value back in payload to the rpc