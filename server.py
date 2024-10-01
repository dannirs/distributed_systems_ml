import socket
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
import os

latest_message = "No messages received yet."
data_store = {}

def start_tcp_server():
    global latest_message
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = ('0.0.0.0', 65432)
    print(f"Starting TCP server on {server_address[0]}:{server_address[1]}")
    server_socket.bind(server_address)
    server_socket.listen(1)

    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        client_handler.start()

def handle_client(client_socket):
    try:
        print(f"Connection established with client")
        while True:
            data = client_socket.recv(1024)
            if data:
                latest_message = data.decode('utf-8')
                print(f"Received: {latest_message}")
                response = "Server received: " + latest_message
                client_socket.sendall(response.encode('utf-8'))
            else:
                break
            # msg_type = client_socket.recv(1024).decode()
            # if msg_type == b'post':
            #     # Receive file metadata
            #     file_name = client_socket.recv(1024).decode()
            #     file_path = f"server_files/{file_name}"
                
            #     # Receive the file from the client
            #     with open(file_path, 'wb') as f:
            #         while True:
            #             file_data = client_socket.recv(1024)
            #             if not file_data:
            #                 break
            #             f.write(file_data)

            #     # Store file metadata in dictionary
            #     file_store[file_name] = file_path
            #     client_socket.send(b'File received and stored')

            # elif msg_type == b'get':
            #     # Receive file name client wants
            #     file_name = client_socket.recv(1024).decode()

            #     # Check if file exists in the store
            #     if file_name in file_store:
            #         file_path = file_store[file_name]

            #         # Send the file to the client
            #         with open(file_path, 'rb') as f:
            #             while True:
            #                 file_data = f.read(1024)
            #                 if not file_data:
            #                     break
            #                 client_socket.send(file_data)

            #         client_socket.send(b'File sent')
            #     else:
            #         client_socket.send(b'File not found')

            # else:
            #     client_socket.send(b'Invalid command')
    finally:
        client_socket.close()

class MyHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        self.wfile.write(f"<html><body><h1>Latest Message: {latest_message}</h1></body></html>".encode('utf-8'))

def start_http_server():
    httpd = HTTPServer(('0.0.0.0', 8080), MyHandler)
    print("Starting HTTP server on port 8080...")
    httpd.serve_forever()

if __name__ == "__main__":
    if not os.path.exists("server_files"):
        os.mkdir("server_files")
    tcp_thread = threading.Thread(target=start_tcp_server)
    tcp_thread.daemon = True
    tcp_thread.start()

    start_http_server()