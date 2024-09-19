import socket

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server_address = ('localhost', 65432)  
    print(f"Starting server on {server_address[0]}:{server_address[1]}")
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
                    print(f"Received: {data.decode('utf-8')}")
                    response = "Server received: " + data.decode('utf-8')
                    connection.sendall(response.encode('utf-8'))
                else:
                    print(f"No more data from {client_address}")
                    break
        finally:
            connection.close()

if __name__ == "__main__":
    start_server()
