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


def _read_one(f):
    c = f.read(1)
    if c == b'':
        raise EOFError("Unexpected EOF while reading bytes")

    return ord(c)


def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    shift = 0
    result = 0

    while True:
        i = _read_one(f)
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
    size = 2048

    def __init__(self, sock):
        self._i = 0
        self.sock = sock
        self.block = None
        super(SockReader, self).__init__()

    def read(self, n):
        if not self.block:
            self.block = bytearray(self.sock.recv(self.size))
            self._i = 0

        rv = self.block[self._i:self._i + n]
        read = len(rv)
        self._i += read

        if n != -1:
            unread = n - read
        else:
            unread = 0

        while unread > 0:
            self.block = bytearray(self.sock.recv(self.size))
            self._i = 0
            part = self.block[self._i:self._i + unread]
            self._i += len(part)
            unread -= len(part)
            rv += part

        return str(rv)

    def close(self):
        pass
