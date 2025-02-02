import struct

def create_kafka_response(request_headers: dict, body):
    """
    Create a Kafka response message with the following structure:
    - message_size (4 bytes)
    - Header
        - correlation_id (4 bytes)
    - Body
    """
    # encode the body (placeholder for not, body is not being used yet)
    body_bytes = b''
    
    # construct header using big-endian byte order
    # i = 4-byte integer
    # h = 2-byte short integer
    header = struct.pack( # used to convert integers to bytes
        '>i',
        request_headers['correlation_id'],
    )
    
    # Combine header and body
    message_content = header + body_bytes
    
    # Prepend with message_size (total length of header + body)
    message_size = len(message_content)
    full_message = struct.pack('>i', message_size) + message_content
    
    return full_message