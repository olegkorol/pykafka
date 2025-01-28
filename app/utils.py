import struct

def create_kafka_response(correlation_id, body):
    """
    Create a Kafka response message with the following structure:
    - Message Size (4 bytes)
    - Header (Correlation ID)
    - Body
    """
    # Encode the body (this is a placeholder - actual encoding depends on specific Kafka protocol)
    body_bytes = body.encode('utf-8')
    
    # Construct header
    header = struct.pack( # used to convert integers to bytes
        '>i',  # Big-endian: int (correlation_id)
        correlation_id
    )
    
    # Combine header and body
    message_content = header + body_bytes
    
    # Prepend message size (total length of header + body)
    message_size = len(message_content)
    full_message = struct.pack('>i', message_size) + message_content
    
    return full_message