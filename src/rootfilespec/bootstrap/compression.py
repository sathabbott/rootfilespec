from __future__ import annotations

from typing import Annotated, Protocol

import cramjam  # type: ignore[import-not-found]

from rootfilespec.structutil import (
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@serializable
class RCompressionHeader(ROOTSerializable):
    """The header of a compressed data payload.

    Attributes:
        fAlgorithm: The compression algorithm used. Possible values are:
            - b"ZL": zlib
            - b"XZ": xz
            - b"L4": lz4
            - b"ZS": zstd
        fVersion: The version of the compression algorithm.
        fCompressedSize: The size of the compressed data, packed into 3 bytes.
            Use compressed_size() to get the actual size.
        fUncompressedSize: The size of the uncompressed data, packed into 3 bytes.
            Use uncompressed_size() to get the actual size.
    """

    fAlgorithm: Annotated[bytes, Fmt("2s")]
    fVersion: Annotated[int, Fmt("B")]
    fCompressedSize: Annotated[bytes, Fmt("3s")]
    fUncompressedSize: Annotated[bytes, Fmt("3s")]

    def compressed_size(self) -> int:
        return sum(s << (8 * i) for i, s in enumerate(self.fCompressedSize))

    def uncompressed_size(self) -> int:
        return sum(s << (8 * i) for i, s in enumerate(self.fUncompressedSize))


class Decompressor(Protocol):
    def __call__(
        self, data: bytes | memoryview, output_len: int | None = None
    ) -> bytes: ...


def get_decompressor(algorithm: bytes) -> Decompressor:
    if algorithm == b"ZL":
        return cramjam.zlib.decompress  # type: ignore[no-any-return]
    if algorithm == b"XZ":
        return cramjam.xz.decompress  # type: ignore[no-any-return]
    if algorithm == b"L4":
        return cramjam.lz4.decompress  # type: ignore[no-any-return]
    if algorithm == b"ZS":
        return cramjam.zstd.decompress  # type: ignore[no-any-return]
    msg = f"Unknown compression algorithm {algorithm!r}"
    raise NotImplementedError(msg)


@serializable
class RCompressed(ROOTSerializable):
    """A compressed data payload.

    Attributes:
        header: The header of the compressed data payload.
        checksum: The checksum of the compressed data payload.
            Only present if the algorithm is L4, as other algorithms have built-in checksums.
        payload: The compressed data payload.
    """

    header: RCompressionHeader
    checksum: bytes | None
    payload: memoryview

    @classmethod
    def read_members(cls, buffer: ReadBuffer):
        header, buffer = RCompressionHeader.read(buffer)
        if header.fAlgorithm == b"L4":
            checksum, buffer = buffer.consume(4)
        else:
            checksum = None
        # Not using .consume() to avoid copying the payload
        nbytes = header.compressed_size()
        payload, buffer = buffer.data[:nbytes], buffer[nbytes:]
        return (header, checksum, payload), buffer

    def decompress(self) -> memoryview:
        decompressor = get_decompressor(self.header.fAlgorithm)
        out = decompressor(self.payload, output_len=self.header.uncompressed_size())
        if self.checksum is not None:
            import xxhash  # type: ignore[import-not-found]

            checksum = xxhash.xxh64(out, seed=0).digest()
            if checksum != self.checksum:
                msg = f"Checksum mismatch: {checksum!r} != {self.checksum!r}"
                raise ValueError(msg)
        return memoryview(out)
