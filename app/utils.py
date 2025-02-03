import struct
from enum import Enum
from app.supported_apis import supported_apis

class ErrorCodes(Enum):
    NO_ERRORS = 0 # custom one
    UNSUPPORTED_VERSION = 35

def create_kafka_response(request_headers: dict, client_address: tuple | None = None):
    """
    Creates an ApiVersionResponse based on the Kafka specification (v4):
        - message_size => INT32
        - Header
            - correlation_id => UINT32
        - Body
            error_code => INT16
            api_versions => ARRAY[ApiVersion]
                api_key => INT16
                min_version => INT16
                max_version => INT16
                TAG_BUFFER => INT16
            throttle_time_ms => INT32

    Big-endian (`!` or `>`) byte order notation:
        B = 1-byte unsigned integer (8 bits, UINT8)
        h = 2-byte short integer (16 bits, INT16)
        i = 4-byte integer (32 bits, INT32)
        I = 4-byte unsigned integer (32 bits, INT32)
    
    FYI: 1 byte = 8 bits
    """

    #############################
    #      CONSTRUCT HEADER     #
    #############################
    header = struct.pack('>I', request_headers['correlation_id'])

    #############################
    #       CONSTRUCT BODY      #
    #############################
    request_api_key = request_headers['request_api_key']
    request_api_version = request_headers['request_api_version']
    is_valid_api_key = request_api_key < len(supported_apis)

    min_ver = supported_apis[request_api_key][1] if is_valid_api_key else 0
    max_ver = supported_apis[request_api_key][2] if is_valid_api_key else 0

    error_code = ErrorCodes.NO_ERRORS if is_valid_api_key and request_api_version in range(min_ver, max_ver + 1) else ErrorCodes.UNSUPPORTED_VERSION

    print(f"[debug - {client_address}]\n 'request_api_key' valid? {is_valid_api_key} -> (min_ver: {min_ver}, max_ver: {max_ver})")
    print(f"[debug - {client_address}]\n response 'error_code': {error_code.value} ({error_code.name})")

    # Start with error code
    body_bytes = struct.pack('!h', error_code.value)

    # Collect API keys to include
    api_keys_to_include = []
    for api in supported_apis:
        if api[0] == request_api_key:
            api_keys_to_include.append(api)
        if api[0] == 75:
            api_keys_to_include.append(api)

    # Number of API keys (must be +1 due to Kafka protocol quirk)
    body_bytes += struct.pack('!B', len(api_keys_to_include) + 1)

    # Pack each API key
    for api in api_keys_to_include:
        api_key, min_version, max_version = api[0], api[1], api[2]
        print(f"Packing API {api_key}: min={min_version}, max={max_version}")
        body_bytes += struct.pack('!hhhB', api_key, min_version, max_version, 0) # with buffer at end
    
    # TAG_BUFFER for API key (INT16)
    body_bytes += struct.pack('!B', 0)

    # Throttle time (INT32)
    body_bytes += struct.pack('!i', 0)

    #############################
    #  CONSTRUCT FULL RESPONSE  #
    #############################
    message_content = header + body_bytes
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
