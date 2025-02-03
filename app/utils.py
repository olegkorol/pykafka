
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
