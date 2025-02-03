import sys
import asyncio
from asyncio import StreamReader, StreamWriter
import configparser
from app.utils import print_hex
from app.kafka import Kafka

async def handle_client(client_reader: StreamReader, client_writer: StreamWriter):
    """Handle communication with a single client

    takes two parameters: client_reader, client_writer.
    
    client_reader is a StreamReader object, while client_writer is a StreamWriter object.
    This parameter can either be a plain callback function or a coroutine; if it is a coroutine, it will be automatically converted into a Task.
    """
    client_address: tuple = client_writer.get_extra_info('peername')

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
            remaining_bytes = message_size_bytes
            request = b''
            while remaining_bytes > 0:
                chunk = await client_reader.read(remaining_bytes)
                request += chunk
                remaining_bytes -= len(chunk)
            print(f"[debug - {client_address}]\n request message:")
            print_hex(request)

            # create and send response
            response = b''
            response += Kafka(request, client_address=client_address).create_response()
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
    if len(sys.argv) > 2:
        print("Usage: ./your_program.sh /tmp/server.properties")
        exit(64)

    server_properties_file_path = None

    if len(sys.argv) == 2:
        server_properties_file_path = sys.argv[1]

        try:
            config = configparser.ConfigParser()
            # read the properties file without requiring section headers
            config.read_string('[DEFAULT]\n' + open(server_properties_file_path).read())
        except Exception as e:
            print(f"[server]\n error: {e}")
            exit(1)

    PORT = int(config.get('DEFAULT', 'port', fallback=9092)) if server_properties_file_path else 9092

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
