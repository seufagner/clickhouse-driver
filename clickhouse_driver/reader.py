from struct import Struct


def read_binary_str(buf):
    length = read_varint(buf)
    return read_binary_str_fixed_len(buf, length)


def read_binary_bytes(buf):
    length = read_varint(buf)
    return read_binary_bytes_fixed_len(buf, length)


def read_binary_str_fixed_len(buf, length):
    return read_binary_bytes_fixed_len(buf, length).decode('utf-8')


def read_binary_bytes_fixed_len(buf, length):
    return buf.read(length)


def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    shift = 0
    result = 0

    while True:
        i = f.read_one()
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result


def read_binary_int(buf, fmt):
    """
    Reads int from buffer with provided format.
    """
    # Little endian.
    s = Struct('<' + fmt)
    return s.unpack(buf.read(s.size))[0]


def read_binary_int8(buf):
    return read_binary_int(buf, 'b')


def read_binary_int16(buf):
    return read_binary_int(buf, 'h')


def read_binary_int32(buf):
    return read_binary_int(buf, 'i')


def read_binary_int64(buf):
    return read_binary_int(buf, 'q')


def read_binary_uint8(buf):
    return read_binary_int(buf, 'B')


def read_binary_uint16(buf):
    return read_binary_int(buf, 'H')


def read_binary_uint32(buf):
    return read_binary_int(buf, 'I')


def read_binary_uint64(buf):
    return read_binary_int(buf, 'Q')


def read_binary_uint128(buf):
    hi = read_binary_int(buf, 'Q')
    lo = read_binary_int(buf, 'Q')

    return (hi << 64) + lo


class SockReader(object):
    def __init__(self, sock, bufsize):
        self.buffer = bytearray(bufsize)
        self.buffer_view = memoryview(self.buffer)

        self.position = 0
        self.sock = sock
        self.current_buffer_size = 0
        super(SockReader, self).__init__()

    def read(self, unread):
        # When the buffer is large enough bytes read are almost always hit the buffer.
        next_position = unread + self.position
        if next_position < self.current_buffer_size:
            t = self.position
            self.position = next_position
            return self.buffer[t:self.position]

        rv = bytearray(unread)
        rv_view = memoryview(rv)
        rv_position = 0

        while unread > 0:
            if self.position == self.current_buffer_size:
                self.current_buffer_size = self.sock.recv_into(self.buffer)
                self.position = 0

            l = min(unread, self.current_buffer_size - self.position)
            rv_view[rv_position:rv_position + l] = self.buffer_view[self.position:self.position + l]
            self.position += l
            rv_position += l
            unread -= l

        return rv

    def read_one(self):
        if self.position == self.current_buffer_size:
            self.current_buffer_size = self.sock.recv_into(self.buffer)
            self.position = 0

        rv = self.buffer[self.position]
        self.position += 1
        return rv

    def close(self):
        pass
