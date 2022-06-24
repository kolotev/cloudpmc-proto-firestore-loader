import io

import zstandard

z_compressor = zstandard.ZstdCompressor(level=10)
z_decompressor = zstandard.ZstdDecompressor()


def compress(data_in: bytes) -> bytes:
    data_out = io.BytesIO()
    with z_compressor.stream_writer(data_out, closefd=False) as s_writer:
        s_writer.write(data_in)
    data_out.seek(0)
    return data_out.read()


def decompress(data: bytes) -> bytes:
    with z_decompressor.stream_reader(io.BytesIO(data)) as s_reader:
        return s_reader.read()
