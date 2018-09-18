
from .. import errors
from ..reader import read_binary_bytes, read_binary_bytes_fixed_len, read_varint
from ..writer import write_binary_bytes, write_binary_bytes_fixed_len, write_varint, _byte
from ..util import compat
from .base import Column

from codecs import utf_8_decode, utf_8_encode


class String(Column):
    ch_type = 'String'
    py_types = compat.string_types

    # TODO: pass user encoding here

    def prepare_null(self, value):
        if self.nullable and value is None:
            return '', True

        else:
            return value, False

    def write_items(self, items, buf):
        for value in items:
            if not isinstance(value, bytes):
                value = utf_8_encode(value)[0]

            write_varint(len(value), buf)
            buf.write(value)

    def read_items(self, n_items, buf):
        items = [None] * n_items
        i = 0
        while i < n_items:
            length = read_varint(buf)
            value = buf.read(length)

            try:
                value = utf_8_decode(value)[0]
            except UnicodeDecodeError:
                # Do nothing. Just return bytes.
                pass

            items[i] = value
            i += 1

        return items


class ByteString(String):
    # TODO: support only bytes

    def prepare_null(self, value):
        if self.nullable and value is None:
            return '', True

        else:
            return value, False

    def write_items(self, items, buf):
        for value in items:
            write_varint(len(value), buf)
            buf.write(value)

    def read_items(self, n_items, buf):
        items = [None] * n_items

        i = 0

        buffer = buf.buffer
        buffer_view = buf.buffer_view
        position = buf.position
        current_buffer_size = buf.current_buffer_size
        sock = buf.sock

        while i < n_items:
            shift = 0
            result = 0

            while True:
                if position == current_buffer_size:
                    current_buffer_size = sock.recv_into(buffer)
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
            if right >= current_buffer_size:
                rv = buffer_view[position:current_buffer_size].tobytes()

                position = right - current_buffer_size
                current_buffer_size = sock.recv_into(buffer)
                rv += buffer_view[0:position].tobytes()

            else:
                rv = buffer_view[position:right].tobytes()
                position += result

            items[i] = rv
            i += 1

        buf.buffer = buffer
        buf.buffer_view = buffer_view
        buf.position = position
        buf.current_buffer_size = current_buffer_size
        # print(items)

        return items


class FixedString(String):
    ch_type = 'FixedString'

    def __init__(self, length, **kwargs):
        self.length = length
        super(FixedString, self).__init__(**kwargs)

    def read_items(self, n_items, buf):
        items = [None] * n_items
        items_buf = buf.read(self.length * n_items)

        i = 0
        buf_pos = 0
        while i < n_items:
            value = items_buf[buf_pos:buf_pos + self.length]
            try:
                value = utf_8_decode(value)[0]
                value = value.rstrip('\x00')
            except UnicodeDecodeError:
                value = value.rstrip(b'\x00')

            items[i] = value
            i += 1
            buf_pos += self.length

        return items

    def write_items(self, items, buf):
        items_buf = bytearray(self.length * len(items))
        items_buf_view = memoryview(items_buf)
        buf_pos = 0

        for value in items:
            if not isinstance(value, bytes):
                value = value.encode('utf-8')

            if self.length < len(value):
                raise errors.TooLargeStringSize()

            items_buf_view[buf_pos:buf_pos + min(self.length, len(value))] = value
            buf_pos += self.length

        buf.write(items_buf)


class ByteFixedString(FixedString):
    def read_items(self, n_items, buf):
        l = self.length
        items = [None] * n_items
        items_buf = buf.read(l * n_items)

        i = 0
        buf_pos = 0
        while i < n_items:
            items[i] = items_buf[buf_pos:buf_pos + l]
            i += 1
            buf_pos += l

        return items

    def write_items(self, items, buf):
        items_buf = bytearray(self.length * len(items))
        items_buf_view = memoryview(items_buf)
        buf_pos = 0

        for value in items:
            if self.length < len(value):
                raise errors.TooLargeStringSize()

            items_buf_view[buf_pos:buf_pos + min(self.length, len(value))] = value
            buf_pos += self.length

        buf.write(items_buf)


def create_string_column(spec, column_options):
    client_settings = column_options['context'].client_settings
    strings_as_bytes = client_settings['strings_as_bytes']

    if spec == 'String':
        if strings_as_bytes:
            return ByteString(**column_options)
        else:
            return String(**column_options)
    else:
        length = int(spec[12:-1])
        if strings_as_bytes:
            return ByteFixedString(length, **column_options)
        else:
            return FixedString(length, **column_options)
