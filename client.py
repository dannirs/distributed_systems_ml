import socket

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server_address = ('localhost', 65432)  
    print(f"Connecting to {server_address[0]}:{server_address[1]}")
    client_socket.connect(server_address)
    
    try:
        message = "Hello, Server!"
        print(f"Sending: {message}")
        client_socket.sendall(message.encode('utf-8'))
        
        data = client_socket.recv(1024)
        print(f"Received from server: {data.decode('utf-8')}")
    
    finally:
        print("Closing connection")
        client_socket.close()

if __name__ == "__main__":
    start_client()