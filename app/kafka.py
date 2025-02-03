from enum import Enum
import struct
from app.supported_apis import supported_apis

class ErrorCodes(Enum):
	NO_ERRORS = 0
	UNSUPPORTED_VERSION = 35
	UNKNOWN_TOPIC_OR_PARTITION = 3

class Kafka():
	_request: bytes
	_client_address: tuple
	_response: bytes

	request_api_key: int
	request_api_version: int
	correlation_id: int
	client_id: str
	_header_ends_at_byte: int # position at which Request Header ends and Body starts
	# DescribeTopicPartitions request body
	topic_names: list[str] = []

	error_code: int
	min_ver: int
	max_ver: int

	# REFERENCE
	# Big-endian (`!` or `>`) byte order notation:
	#     b = 1-byte integer (8 bits, INT8)
    #     B = 1-byte unsigned integer (8 bits, UINT8)
    #     h = 2-byte short integer (16 bits, INT16)
    #     i = 4-byte integer (32 bits, INT32)
    #     I = 4-byte unsigned integer (32 bits, INT32)
	
	def __init__(self, request: bytes, client_address: tuple):
		self._request = request
		self._client_address = client_address
		self._parse_common_headers()

	def create_response(self) -> bytes:
		match self.request_api_key:
			case 18:
				self._create_api_version_response()
			case 75:
				self._parse_describe_topic_partitions_request_body()
				self._create_describe_topic_partitions_response()
			case _:
				# Ignore for now
				pass
		
		return self._response
		
	def _parse_common_headers(self):
		self.request_api_key = int.from_bytes(self._request[0:2], byteorder='big')
		self.request_api_version = int.from_bytes(self._request[2:4], byteorder='big')
		self.correlation_id = int.from_bytes(self._request[4:8], byteorder='big', signed=False)

		client_id_length = int.from_bytes(self._request[8:10], byteorder='big') # 2 bytes
		client_id = self._request[10:(10 + client_id_length)]
		self.client_id = client_id.decode('utf-8')

		print(f'[debug - {self._client_address}]\n'
		      f'parsed headers:\n'
		      f'    - request_api_key: {self.request_api_key}\n'
		      f'    - request_api_version: {self.request_api_version}\n'
		      f'    - correlation_id: {self.correlation_id}\n'
		      f'    - client_id: {self.client_id}')

		self._header_ends_at_byte = (10 + client_id_length + 1) # byte position after client_id + tag buffer

		# assert request validity, based on supported api keys and their versions
		self.is_valid_api_key = self.request_api_key < len(supported_apis)
		self.min_ver = supported_apis[self.request_api_key][1] if self.is_valid_api_key else 0
		self.max_ver = supported_apis[self.request_api_key][2] if self.is_valid_api_key else 0

		print(f'[debug - {self._client_address}]\n'
		      f"'request_api_key' valid? {self.is_valid_api_key}\n"
		      f'(min_ver: {self.min_ver}, max_ver: {self.max_ver})')

	def _parse_describe_topic_partitions_request_body(self):
		body_start = self._header_ends_at_byte
		current_position = body_start

		topics_length = int.from_bytes(self._request[body_start:body_start + 1]) - 1 # 1 byte COMPACT_ARRAY
		current_position += 1

		for _ in range(topics_length):
			topic_name_length = int.from_bytes(self._request[current_position:(current_position + 1)]) - 1 # 1 byte VARINT
			current_position += 1
			topic_name = self._request[current_position:(current_position + topic_name_length)]
			self.topic_names.append(topic_name.decode('utf-8'))
			current_position += topic_name_length + 1 # +1 to account for tag buffer

		# ignoring the rest of the payload for now...

		print(f'[debug - {self._client_address}]\n'
			  f'parsed body:\n'
			  f'	- topic_names: {self.topic_names}')

	def _get_error_code(self):
		self.error_code = (
			ErrorCodes.NO_ERRORS 
			if self.is_valid_api_key and 
				self.request_api_version in range(self.min_ver, self.max_ver + 1) 
			else ErrorCodes.UNSUPPORTED_VERSION
		)
		print(f"[debug - {self._client_address}]\n response 'error_code': {self.error_code.value} ({self.error_code.name})")

		# return only the value of the error-code
		return self.error_code.value

	def _create_api_version_response(self):
		"""ApiVersion Response v4
		"""
		#############################
		#      CONSTRUCT HEADER     #
		#############################

		# => correlation_id (4 bytes)
		header = struct.pack('>I', self.correlation_id)

		#############################
		#       CONSTRUCT BODY      #
		#############################

		# => error_code (2 bytes)
		body_bytes = struct.pack('!h', self._get_error_code())

		api_keys_to_include = []
		for api in supported_apis:
			if api[0] == self.request_api_key:
				api_keys_to_include.append(api)
			if api[0] == 75:
				api_keys_to_include.append(api)

		# => api_keys (1 byte)
		# This array is encoded as a COMPACT_ARRAY, which starts with a varint corresponding to the length of the array + 1, followed by each element, defined next
		body_bytes += struct.pack('!B', len(api_keys_to_include) + 1)

		# => api_version (7 bytes) * N api_keys
		# Each api_version contains: api_key (2 bytes), min_version (2 bytes), max_version (2 bytes), tag buffer (1 byte)
		for api in api_keys_to_include:
			api_key, min_version, max_version = api
			body_bytes += struct.pack('!hhhB', api_key, min_version, max_version, 0) # with buffer at end
		
		# => tag buffer (1 byte)
		body_bytes += struct.pack('!B', 0)

		# => throttle_time_ms (4 bytes)
		body_bytes += struct.pack('!i', 0)

		#############################
		#  CONSTRUCT FULL RESPONSE  #
		#############################
		message_content = header + body_bytes
		# => message_size (4 bytes)
		message_size = len(message_content)
		full_message = struct.pack('!i', message_size) + message_content

		self._response = full_message

	def _create_describe_topic_partitions_response(self):
		#############################
		#      CONSTRUCT HEADER     #
		#############################

		# => correlation_id (4 bytes)
		header = struct.pack('>IB', self.correlation_id, 0) # with tag buffer at end
		
		#############################
		#       CONSTRUCT BODY      #
		#############################

		# => throttle_time_ms (4 bytes)
		body_bytes = struct.pack('!i', 0)

		# topics_to_include = []
		# for api in supported_apis:
		# 	if api[0] == self.request_api_key:
		# 		topics_to_include.append(api)
		# 	if api[0] == 75:
		# 		topics_to_include.append(api)

		# => topics (1 byte)
		# This array is encoded as a COMPACT_ARRAY, which starts with a varint corresponding to the length of the array + 1, followed by each element, defined next
		body_bytes += struct.pack('!B', len(self.topic_names) + 1)

		# => topic (29 bytes) * N topics
		# Each topic contains: error_code (2 bytes), topic_name (4 bytes), topic_id (16 bytes), is_internal (1 byte), partitions_array (1 byte),
		#                      topic_authorized_operations (4 bytes), tag buffer (1 byte)
		for topic_name in self.topic_names:
			(3, topic_name, '00000000-0000-0000-0000-000000000000', 0, 0, 0) # TODO: This is almost all hardcoded for now...

			body_bytes += struct.pack('!h', ErrorCodes.UNKNOWN_TOPIC_OR_PARTITION.value) # error_code
			body_bytes += struct.pack('!B', len(topic_name) + 1) # topic_name_length
			body_bytes += topic_name.encode('utf-8') # topic_name (direct bytes, no hex encoding)
			body_bytes += bytes.fromhex('00000000-0000-0000-0000-000000000000'.replace('-', '')) # topic_id (direct bytes)
			body_bytes += struct.pack('!B', 0) # is_internal
			body_bytes += struct.pack('!B', 0 + 1) # partitions_array (should have + 1)
			body_bytes += struct.pack('!i', 0000_0000_0000_0000) # topic_authorized_operations
			body_bytes += struct.pack('!B', 0) # tag buffer
		
		# => next_cursor
		body_bytes += struct.pack('!b', -1) # representing null

		# => tag buffer (1 byte)
		body_bytes += struct.pack('!B', 0)

		#############################
		#  CONSTRUCT FULL RESPONSE  #
		#############################
		message_content = header + body_bytes
		# => message_size (4 bytes)
		message_size = len(message_content)
		full_message = struct.pack('!i', message_size) + message_content

		self._response = full_message