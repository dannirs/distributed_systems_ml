import socket
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer

# Global variable to store the latest message received from the client
latest_message = "No messages received yet."

# TCP Server to handle client connections
def start_tcp_server():
    global latest_message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', 65432)
    print(f"Starting TCP server on {server_address[0]}:{server_address[1]}")
    server_socket.bind(server_address)
    server_socket.listen(1)

    while True:
        print("Waiting for a connection...")
        connection, client_address = server_socket.accept()
        try:
            print(f"Connection established with {client_address}")
            while True:
                data = connection.recv(1024)
                if data:
                    latest_message = data.decode('utf-8')
                    print(f"Received: {latest_message}")
                    response = "Server received: " + latest_message
                    connection.sendall(response.encode('utf-8'))
                else:
                    print(f"No more data from {client_address}")
                    break
        finally:
            connection.close()

# HTTP Server to serve the latest message on localhost:65432
class MyHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        # Send a 200 OK response
        self.send_response(200)
        # Set the content type to text/html
        self.send_header("Content-type", "text/html")
        self.end_headers()
        # Display the latest message received from the client
        self.wfile.write(f"<html><body><h1>Latest Message: {latest_message}</h1></body></html>".encode('utf-8'))

def start_http_server():
    httpd = HTTPServer(('0.0.0.0', 8080), MyHandler)
    print("Starting HTTP server on port 65432...")
    httpd.serve_forever()

if __name__ == "__main__":
    # Run the TCP server in a separate thread
    tcp_thread = threading.Thread(target=start_tcp_server)
    tcp_thread.daemon = True
    tcp_thread.start()

    # Run the HTTP server in the main thread
    start_http_server()