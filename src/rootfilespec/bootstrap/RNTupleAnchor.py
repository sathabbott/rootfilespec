from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)

from .streamedobject import StreamHeader

@structify(big_endian=True)
@dataclass
class RNTupleAnchor_header(StructClass):
    """ A class representing the RNTuple Anchor header structure

    Attributes:
        version_epoch (int): Version Epoch
        version_major (int): Version Major
        version_minor (int): Version Minor
        version_patch (int): Version Patch
        seek_header (int): Offset to the header envelope
        nbytes_header (int): Compressed size of the header envelope
        len_header (int): Uncompressed size of the header envelope
        seek_footer (int): Offset to the footer envelope
        nbytes_footer (int): Compressed size of the footer envelope
        len_footer (int): Uncompressed size of the footer envelope
        max_key_size (int): Maximum size of an RBlob
    """

    version_epoch: int = sfield("H")
    version_major: int = sfield("H")
    version_minor: int = sfield("H")
    version_patch: int = sfield("H")
    seek_header: int = sfield("Q")
    nbytes_header: int = sfield("Q")
    len_header: int = sfield("Q")
    seek_footer: int = sfield("Q")
    nbytes_footer: int = sfield("Q")
    len_footer: int = sfield("Q")
    max_key_size: int = sfield("Q")


@dataclass
class RNTupleAnchor(ROOTSerializable):
    """ RNTuple Anchor object

    Attributes:
        header (RNTupleAnchor_header): RNTuple Anchor header information
        padding (bytes): Padding after the Anchor
        checksum (int): Checksum of the Anchor
    """

    sheader: StreamHeader
    header: RNTupleAnchor_header
    padding: bytes
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\tReading RNTupleAnchor; {buffer.info()}\033[0m")

        # Read the StreamHeader (every named class has a StreamHeader)
        sheader, buffer = StreamHeader.read(buffer)

        # Read the RNTupleAnchor header (everything but the padding and checksum)
        header, buffer = RNTupleAnchor_header.read(buffer)
        # print(header)

        # Unknown information after the Anchor should be ignored (assign to padding)
        # There is an 8 byte checksum appended to the Anchor when writing to disk
        #       So, the last 8 bytes of the Anchor are the checksum (after any padding)

        # print(f"anchor, before padding and checksum: {buffer.info()}")
        padding, buffer = buffer.unpack(f">{buffer.__len__() - 8}s")
        padding = padding[0]
        # print(f"anchor, after padding: {buffer.info()}")
        
        # Get the checksum
        checksum, buffer = buffer.unpack(">Q")
        checksum = checksum[0]
        # print(f"anchor, after checksum: {buffer.info()}")
        # print(f"checksum: {checksum}")

        print(f"\033[1;32m\tDone reading RNTupleAnchor\n\033[0m")
        return cls(sheader, header, padding, checksum), buffer
