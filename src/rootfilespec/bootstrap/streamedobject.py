from __future__ import annotations

from enum import IntEnum

from rootfilespec.bootstrap.TKey import DICTIONARY
from rootfilespec.structutil import ReadBuffer, ROOTSerializable, serializable


class constants(IntEnum):
    """Constants used for data members of StreamedObject class.

    Attributes:
        kByteCountMask (int): Mask for ByteCount
        kClassMask (int): Mask for ClassInfo
        kNewClassTag (int): New class tag
        kNotAVersion (int): Either kClassMask or kNewClassTag is set in bytes 4-5
    """

    kByteCountMask = 0x40000000
    kClassMask = 0x80000000
    kNewClassTag = 0xFFFFFFFF
    kNotAVersion = 0x8000


@serializable
class StreamHeader(ROOTSerializable):
    """Initial header for any streamed data object

    Reference: https://root.cern/doc/master/dobject.html

    Attributes:
        fByteCount (int): Number of remaining bytes in object (uncompressed)
        fVersion (int): Version of Class
        fClassRef (bytes | int | None): Reference to class info, either by name or by index in record
        remaining (int): Number of remaining bytes in object
    """

    fByteCount: int
    fVersion: int | None
    fClassName: bytes | None
    fClassRef: int | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (fByteCount, tmp), buffer = buffer.unpack(">iH")
        if fByteCount & constants.kByteCountMask:
            fByteCount &= ~constants.kByteCountMask
        else:
            msg = f"ByteCount mask not set: {fByteCount:08x}"
            msg += f"\nfByteCount: {fByteCount}, tmp: {tmp:04x}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        fVersion, fClassName, fClassRef = None, None, None
        if not (tmp & constants.kNotAVersion):
            fVersion = tmp
        else:
            fVersion = None
            (more,), buffer = buffer.unpack(">H")
            fClassInfo: int = (tmp << 16) | more
            if fClassInfo == constants.kNewClassTag:
                fClassRef = buffer.relpos - 4
                fClassName = b""
                while True:
                    chr, buffer = buffer.consume(1)
                    if chr == b"\0":
                        break
                    fClassName += chr
                buffer.local_refs[fClassRef] = fClassName
            else:
                fClassRef = (fClassInfo & ~constants.kClassMask) - 2
                fClassName = buffer.local_refs.get(fClassRef, None)
                if fClassName is None:
                    msg = f"ClassRef {fClassRef} not found in buffer local_refs"
                    raise ValueError(msg)
        return cls(fByteCount, fVersion, fClassName, fClassRef), buffer


def read_streamed_item(buffer: ReadBuffer) -> tuple[ROOTSerializable, ReadBuffer]:
    # Read ahead the stream header to determine the type of the object
    itemheader, _ = StreamHeader.read(buffer)
    if itemheader.fClassName not in DICTIONARY:
        msg = f"Unknown class name: {itemheader.fClassName}"
        msg += f"\nStreamHeader: {itemheader}"
        raise ValueError(msg)
    expected_position = buffer.relpos + 4 + itemheader.fByteCount
    # Now actually read the object
    item, buffer = DICTIONARY[itemheader.fClassName].read(buffer)
    if buffer.relpos != expected_position:
        msg = f"Expected position {expected_position} but got {buffer.relpos}"
        msg += f"\nItem: {item}"
        msg += f"\nBuffer: {buffer}"
        raise ValueError(msg)
    return item, buffer
