from codecs import utf_8_decode


class BufferedReader(object):
    def __init__(self, bufsize):
        self.buffer = bytearray(bufsize)
        self.buffer_view = memoryview(self.buffer)

        self.position = 0
        self.current_buffer_size = 0

        super(BufferedReader, self).__init__()

    def read_into_buffer(self):
        raise NotImplementedError

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
                self.read_into_buffer()
                self.position = 0

            read_bytes = min(unread, self.current_buffer_size - self.position)
            rv_view[rv_position:rv_position + read_bytes] = self.buffer_view[self.position:self.position + read_bytes]
            self.position += read_bytes
            rv_position += read_bytes
            unread -= read_bytes

        return rv

    def read_one(self):
        if self.position == self.current_buffer_size:
            self.read_into_buffer()
            self.position = 0

            if self.current_buffer_size == 0:
                raise EOFError("Unexpected EOF while reading bytes")

        rv = self.buffer[self.position]
        self.position += 1
        return rv

    def read_strings(self, n_items, decode=None):
        """
        Python has great overhead between function calls.
        We inline strings reading logic here to avoid this overhead.
        """
        items = [None] * n_items

        i = 0

        buffer = self.buffer
        buffer_view = self.buffer_view
        position = self.position
        current_buffer_size = self.current_buffer_size

        while i < n_items:
            shift = 0
            result = 0

            while True:
                if position == current_buffer_size:
                    self.read_into_buffer()
                    buffer = self.buffer
                    buffer_view = self.buffer_view
                    current_buffer_size = self.current_buffer_size
                    position = 0

                b = buffer[position]

                position += 1

                result |= (b & 0x7f) << shift
                shift += 7
                if not (b & 0x80):
                    break

            right = position + result

            # Memory view here is a trade off between speed and memory.
            # Without memory view there will be additional memory fingerprint.
            if right > current_buffer_size:
                rv = buffer_view[position:current_buffer_size].tobytes()

                position = right - current_buffer_size
                self.read_into_buffer()
                buffer = self.buffer
                buffer_view = self.buffer_view
                current_buffer_size = self.current_buffer_size
                rv += buffer_view[0:position].tobytes()

            else:
                rv = buffer_view[position:right].tobytes()
                position += result

            if decode:
                try:
                    rv = utf_8_decode(rv)[0]
                except UnicodeDecodeError:
                    # Do nothing. Just return bytes.
                    pass

            items[i] = rv
            i += 1

        self.buffer = buffer
        self.buffer_view = buffer_view
        self.position = position
        self.current_buffer_size = current_buffer_size

        return items

    def close(self):
        pass


class BufferedSocketReader(BufferedReader):
    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.current_buffer_size = self.sock.recv_into(self.buffer)


class CompressedBufferedReader(BufferedReader):
    def __init__(self, read_block, bufsize):
        self.read_block = read_block
        super(CompressedBufferedReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.buffer = bytearray(self.read_block())
        self.buffer_view = memoryview(self.buffer)
        self.current_buffer_size = len(self.buffer)
