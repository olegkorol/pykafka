import struct
from enum import Enum

# supported API keys and their version ranges
supported_apis = [
    # (api_key, min_version, max_version)
    (0, 0, 4),
    (1, 0, 4),
    (2, 0, 4),
    (3, 0, 4),
    (4, 0, 4),
    (5, 0, 4),
    (6, 0, 4),
    (7, 0, 4),
    (8, 0, 4),
    (9, 0, 4),
    (10, 0, 4),
    (11, 0, 4),
    (12, 0, 4),
    (13, 0, 4),
    (14, 0, 4),
    (15, 0, 4),
    (16, 0, 4),
    (17, 0, 4),
    (18, 0, 4),
]

class ErrorCodes(Enum):
    NO_ERRORS = 0 # custom one
    UNSUPPORTED_VERSION = 35

def create_kafka_response(request_headers: dict, client_address):
    """
    Creates an ApiVersionResponse based on the Kafka specification (v4):
        - message_size => INT32
        - Header
            - correlation_id => INT32
        - Body
            error_code => INT16
            api_keys => UINT8
                api_key => INT16
                min_version => INT16
                max_version => INT16
                TAG_BUFFER => INT16
            throttle_time_ms => INT32

    Big-endian (`!` or `>`) byte order notation:
        B = 1-byte unsigned integer (8 bits, UINT8)
        h = 2-byte short integer (16 bits, INT16)
        i = 4-byte integer (32 bits, INT32)
    
    FYI: 1 byte = 8 bits
    """

    #############################
    #      CONSTRUCT HEADER     #
    #############################
    header = struct.pack( # used to convert integers to bytes
        '>i',
        request_headers['correlation_id'],
    )

    #############################
    #       CONSTRUCT BODY      #
    #############################
    request_api_key = request_headers['request_api_key']
    request_api_version = request_headers['request_api_version']
    is_valid_api_key = request_api_key < len(supported_apis)

    min_ver = supported_apis[request_api_key][1] if is_valid_api_key else 0
    max_ver = supported_apis[request_api_key][2] if is_valid_api_key else 0

    error_code = ErrorCodes.NO_ERRORS if is_valid_api_key and request_api_version in range(min_ver, max_ver + 1) else ErrorCodes.UNSUPPORTED_VERSION

    print(f"[debug client:{client_address}]\n 'request_api_key' valid? {is_valid_api_key} -> (min_ver: {min_ver}, max_ver: {max_ver})")
    print(f"[debug client:{client_address}]\n response 'error_code': {error_code.value} ({error_code.name})")

    body_bytes = struct.pack('!h', error_code.value)  # error_code (INT16)
    
    # Number of API keys (VARINT, empirically 1 + 1)
    body_bytes += struct.pack('!B', 1 + 1)  # 1 + 1 as a single byte
    
    # API key details
    for api in supported_apis:
        if api[0] == request_api_key: # passing only data for the request_api_key (just for now?)
            body_bytes += struct.pack('!hhh', *api)  # api_key, min_version, max_version
            # TAG_BUFFER for API key (INT16)
            body_bytes += struct.pack('!h', 0)
    
    # Throttle time (INT32)
    body_bytes += struct.pack('!i', 0)
    
    #############################
    #  CONSTRUCT FULL RESPONSE  #
    #############################
    message_content = header + body_bytes
    
    # Prepend with message_size (total length of header + body)
    message_size = len(message_content)
    full_message = struct.pack('!i', message_size) + message_content
    
    return full_message

def print_hex(data, bytes_per_line=16, with_ascii=False):
    hex_str = data.hex()
    for i in range(0, len(hex_str), bytes_per_line * 2):
        # Print hex values
        line = hex_str[i:i + bytes_per_line * 2]
        print(' '.join(line[j:j+2] for j in range(0, len(line), 2)))

        if with_ascii == True:
            # Print ASCII representation
            ascii_line = ''.join(chr(int(line[j:j+2], 16)) if 32 <= int(line[j:j+2], 16) <= 126 else '.' for j in range(0, len(line), 2))
            print(' ' * (bytes_per_line * 3 - len(line)) + '|' + ascii_line + '|')
