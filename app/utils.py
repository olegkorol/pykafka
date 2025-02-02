import struct
from enum import Enum

# Supported ApiVersion
supported_api_versions = [0, 1, 2, 3, 4]

class ErrorCodes(Enum):
    NO_ERRORS = 0 # custom one
    UNSUPPORTED_VERSION = 35

def create_kafka_response(request_headers: dict):
    """
    Create a Kafka response message with the following structure:
        - message_size (4 bytes)
        - Header
            - correlation_id (uint32 = 4 bytes)
        - Body
            - error_code (uint16 = 2 bytes)

    Big-endian byte order notation:
        i = 4-byte integer (32 bits, 4 bytes)
        h = 2-byte short integer (16 bits, 2 bytes)
    
    FYI: 1 byte = 8 bits
    """    
    # construct header using big-endian byte order
    header = struct.pack( # used to convert integers to bytes
        '>i',
        request_headers['correlation_id'],
    )

    # construct body
    error_code = ErrorCodes.NO_ERRORS

    if (request_headers['request_api_version'] not in supported_api_versions):
        error_code = ErrorCodes.UNSUPPORTED_VERSION

    print(f"response error_code: {error_code.value} ({error_code.name})")

    # encode the body (placeholder for not, body is not being used yet)
    body_bytes = struct.pack(
        '>h',
        error_code.value,
    )
    
    # Combine header and body
    message_content = header + body_bytes
    
    # Prepend with message_size (total length of header + body)
    message_size = len(message_content)
    full_message = struct.pack('>i', message_size) + message_content
    
    return full_message