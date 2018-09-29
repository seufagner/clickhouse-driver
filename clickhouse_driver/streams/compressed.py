from _codecs import utf_8_decode
from io import BytesIO

try:
    from clickhouse_cityhash.cityhash import CityHash128
except ImportError:
    raise RuntimeError(
        'Package clickhouse-cityhash is required to use compression'
    )

from .native import BlockOutputStream, BlockInputStream
from ..reader import read_binary_uint8, read_binary_uint128
from ..writer import write_binary_uint8, write_binary_uint128
from ..compression import get_decompressor_cls


class CompressedBlockOutputStream(BlockOutputStream):
    def __init__(self, compressor_cls, compress_block_size, fout, context):
        self.compressor_cls = compressor_cls
        self.compress_block_size = compress_block_size
        self.raw_fout = fout

        self.compressor = self.compressor_cls()
        super(CompressedBlockOutputStream, self).__init__(self.compressor,
                                                          context)

    def reset(self):
        self.compressor = self.compressor_cls()
        self.fout = self.compressor

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def finalize(self):
        compressed = self.get_compressed()
        compressed_size = len(compressed)

        compressed_hash = self.get_compressed_hash(compressed)
        write_binary_uint128(compressed_hash, self.raw_fout)

        block_size = self.compress_block_size

        i = 0
        while i < compressed_size:
            self.raw_fout.write(compressed[i:i + block_size])
            i += block_size

        self.raw_fout.flush()

    def get_compressed(self):
        compressed = BytesIO()

        if self.compressor.method_byte is not None:
            write_binary_uint8(self.compressor.method_byte, compressed)
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        data = self.compressor.get_compressed_data(extra_header_size)
        compressed.write(data)

        return compressed.getvalue()


class CompressedBlockReader(object):
    def __init__(self, read_block):
        self.read_block = read_block

        self.buffer = None
        self.buffer_view = None

        self.position = 0
        self.current_buffer_size = 0

        super(CompressedBlockReader, self).__init__()

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
                self.buffer = bytearray(self.read_block())
                self.buffer_view = memoryview(self.buffer)
                self.current_buffer_size = len(self.buffer)
                self.position = 0

            l = min(unread, self.current_buffer_size - self.position)
            rv_view[rv_position:rv_position + l] = self.buffer_view[self.position:self.position + l]
            self.position += l
            rv_position += l
            unread -= l

        return rv

    def read_one(self):
        return self.read(1)[0]

    def recv_into(self):
        self.buffer = bytearray(self.read_block())
        self.buffer_view = memoryview(self.buffer)
        self.current_buffer_size = len(self.buffer)
        self.position = 0

        return self.current_buffer_size

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
                    self.recv_into()
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
                self.recv_into()
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


class CompressedBlockInputStream(BlockInputStream):
    def __init__(self, fin, context):
        self.raw_fin = fin
        fin = CompressedBlockReader(self.read_block)
        super(CompressedBlockInputStream, self).__init__(fin, context)

    def get_compressed_hash(self, data):
        return CityHash128(data)

    def read_block(self):
        compressed_hash = read_binary_uint128(self.raw_fin)
        method_byte = read_binary_uint8(self.raw_fin)

        decompressor_cls = get_decompressor_cls(method_byte)
        decompressor = decompressor_cls(self.raw_fin)

        if decompressor.method_byte is not None:
            extra_header_size = 1  # method
        else:
            extra_header_size = 0

        return decompressor.get_decompressed_data(
            method_byte, compressed_hash, extra_header_size
        )
