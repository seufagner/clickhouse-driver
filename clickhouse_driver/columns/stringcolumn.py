
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
        return buf.read_strings(n_items, decode=True)


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
        return buf.read_strings(n_items)


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
            value = value.rstrip(b'\x00')
            try:
                value = utf_8_decode(value)[0]
            except UnicodeDecodeError:
                pass

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
    decode_strings = client_settings['decode_strings']

    if spec == 'String':
        cls = String if decode_strings else ByteString
        return cls(**column_options)
    else:
        length = int(spec[12:-1])
        cls = FixedString if decode_strings else ByteFixedString
        return cls(length, **column_options)
