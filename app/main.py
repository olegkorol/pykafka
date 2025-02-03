import sys
import asyncio
from asyncio import StreamReader, StreamWriter
from app.utils import create_kafka_response, print_hex

async def handle_client(client_reader: StreamReader, client_writer: StreamWriter):
    """Handle communication with a single client

    takes two parameters: client_reader, client_writer.
    
    client_reader is a StreamReader object, while client_writer is a StreamWriter object.
    This parameter can either be a plain callback function or a coroutine; if it is a coroutine, it will be automatically converted into a Task.
    """
    client_address = client_writer.get_extra_info('peername')

    try:
        while True:
            # get message_size from requesy (found in the first 4 bytes)
            # https://kafka.apache.org/protocol.html#protocol_common
            message_size_bytes = int.from_bytes(await client_reader.read(4))
            if message_size_bytes == 0:  # Client closed connection
                print(f"[client - {client_address}]\n closed connection")
                break
            print(f"[debug - {client_address}]\n request 'message_size' (bytes): {message_size_bytes}")

            # receive the rest of data
            # taking into account that:
            #   - TCP sockets are byte streams with no message boundaries
            #   - recv(n) can return up to n bytes, but might return fewer
            # ...hence, this loop handles cases when we receive partial data
            remaining_bytes = message_size_bytes
            request = b''
            while remaining_bytes > 0:
                chunk = await client_reader.read(remaining_bytes)
                request += chunk
                remaining_bytes -= len(chunk)
            print(f"[debug - {client_address}]\n request message:")
            print_hex(request)

            # unpack headers from request
            request_api_key = int.from_bytes(request[0:2], byteorder='big')
            request_api_version = int.from_bytes(request[2:4], byteorder='big')
            correlation_id = int.from_bytes(request[4:8], byteorder='big', signed=False)

            request_headers = {
                'request_api_key': request_api_key,
                'request_api_version': request_api_version,
                'correlation_id': correlation_id,
            }

            print(f"[debug - {client_address}]\n request headers: {request_headers}")

            # create and send response
            response = b''
            response += create_kafka_response(request_headers, client_address)
            print(f"[debug - {client_address}]\n response message:")
            print_hex(response)

            client_writer.write(response)
            print(f"[server]\n sent response to client")
    except Exception as e:
        print(f"[server]\n exception while handling client: {client_address}:\n {e}", file=sys.stderr)
    finally:
        client_writer.close()
        await client_writer.wait_closed()
        print("[server]\n closed connection")

async def main():
    PORT = 9092
    # server = socket.create_server(("localhost", PORT), reuse_port=True)
    server = await asyncio.start_server(
        handle_client,
        "localhost",
        PORT,
    )
    print(f"[server]\n started on port {PORT} and listening for connections\n")

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[server]\n shutting down...")
