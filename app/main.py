import sys
import socket
from app.utils import create_kafka_response, print_hex

def handle_client(client_socket, client_address):
    """Handle communication with a single client"""
    try:
        while True:
            # get message_size from requesy (found in the first 4 bytes)
            # https://kafka.apache.org/protocol.html#protocol_common
            message_size_bytes = int.from_bytes(client_socket.recv(4))
            if message_size_bytes == 0:  # Client closed connection
                print(f"[client:{client_address}]\n closed connection")
                break
            print(f"[debug client:{client_address}]\n request 'message_size' (bytes): {message_size_bytes}")

            # receive the rest of data
            # taking into account that:
            #   - TCP sockets are byte streams with no message boundaries
            #   - recv(n) can return up to n bytes, but might return fewer
            # ...hence, this loop handles cases when we receive partial data
            remaining_bytes = message_size_bytes
            request = b''
            while remaining_bytes > 0:
                chunk = client_socket.recv(remaining_bytes)
                request += chunk
                remaining_bytes -= len(chunk)
            print(f"[debug client:{client_address}]\n request message:")
            print_hex(request)

            # extract headers from request
            request_api_key = int.from_bytes(request[0:2], byteorder='big')
            request_api_version = int.from_bytes(request[2:4], byteorder='big')
            correlation_id = int.from_bytes(request[4:8], byteorder='big')

            request_headers = {
                'request_api_key': request_api_key,
                'request_api_version': request_api_version,
                'correlation_id': correlation_id,
            }

            print(f"[debug client:{client_address}]\n request headers: {request_headers}")

            # create and send response
            response = b''
            response += create_kafka_response(request_headers, client_address)
            print(f"[debug client:{client_address}]\n response message:")
            print_hex(response)

            client_socket.sendall(response)
            print(f"[server]\n sent response to client: {client_address}")
    except Exception as e:
        print(f"[server]\n error: {e}", file=sys.stderr)
    finally:
        client_socket.close()
        print("[server]\n closed connection")

def main():
    PORT = 9092
    server = socket.create_server(("localhost", PORT), reuse_port=True)
    print(f"[server]\n started on port {PORT} and listening for connections\n")

    try:
        while True:
            client_socket, client_address = server.accept()
            print(f"[server]\n connected to client: {client_address}")
            handle_client(client_socket, client_address)
    except KeyboardInterrupt:
        print("[server]\n shutting down...")
    finally:
        server.close()
        print("[server]\n connection closed")

if __name__ == "__main__":
    main()
