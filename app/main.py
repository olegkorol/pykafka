import sys
import socket
from app.utils import create_kafka_response

def main():    
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Logs from your program will appear here!")

    client_socket, client_address = server.accept()
    print(f"Connected to client: {client_address}")

    try:
        data = client_socket.recv(1024).decode("utf-8")
        print(f"Received: {data}")

        correlation_id = 7

        response = create_kafka_response(correlation_id, "")

        client_socket.send(response)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        client_socket.close()
        # server.close()

if __name__ == "__main__":
    main()
