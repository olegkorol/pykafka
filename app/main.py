import sys
import socket
from app.utils import create_kafka_response

def main():    
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Logs from your program will appear here!")

    client_socket, client_address = server.accept()
    print(f"Connected to client: {client_address}")

    try:
        # get message_size from requesy (found in the first 4 bytes)
        # https://kafka.apache.org/protocol.html#protocol_common
        message_size = int.from_bytes(client_socket.recv(4))
        print(f"message_size: {message_size}")

        # receive the rest of data
        # taking into account that:
        #   - TCP sockets are byte streams with no message boundaries
        #   - recv(n) can return up to n bytes, but might return fewer
        # ...hence, this loop handles cases when we receive partial data
        remaining_bytes = message_size - 4
        data = b''
        while remaining_bytes > 0:
            chunk = client_socket.recv(remaining_bytes)
            data += chunk
            remaining_bytes -= len(chunk)
        print(f"received raw data: {data}")

        # extract headers
        request_api_key = int.from_bytes(data[0:2], byteorder='big')
        request_api_version = int.from_bytes(data[2:4], byteorder='big')
        correlation_id = int.from_bytes(data[4:8], byteorder='big')

        request_headers = {
            'request_api_key': request_api_key,
            'request_api_version': request_api_version,
            'correlation_id': correlation_id,
        }

        print(request_headers)

        # create and send response
        response = create_kafka_response(request_headers)

        client_socket.sendall(response)
        client_socket.shutdown(socket.SHUT_WR)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        client_socket.close()

if __name__ == "__main__":
    main()
