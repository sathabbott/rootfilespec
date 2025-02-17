from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from ..structutil import ReadContext, ROOTSerializable, read_as


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


@dataclass
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
    fClassRef: bytes | int | None
    remaining: int

    @classmethod
    def read(cls, buffer: memoryview, _: ReadContext):
        (fByteCount, tmp), buffer = read_as(">iH", buffer)
        if fByteCount & constants.kByteCountMask:
            fByteCount &= ~constants.kByteCountMask
        else:
            msg = f"ByteCount mask not set: {fByteCount:08x}"
            raise ValueError(msg)
        if not (tmp & constants.kNotAVersion):
            fVersion = tmp
            fClassRef = None
            remaining = fByteCount - 2
        else:
            fVersion = None
            (more,), buffer = read_as(">H", buffer)
            fClassInfo: int = (tmp << 16) | more
            if fClassInfo == constants.kNewClassTag:
                fClassRef = b""
                while buffer[0] != 0:
                    fClassRef += bytes(buffer[:1])
                    buffer = buffer[1:]
                buffer = buffer[1:]  # skip the null terminator
                # abspos = context.key_length
                # we have read len(fClassRef) + 5 bytes at this point
                # but ROOT seems to not count the null terminator in the byte count
                remaining = fByteCount - len(fClassRef) - 4
            else:
                fClassRef = (fClassInfo & ~constants.kClassMask) - 2  # type: ignore[assignment]
                # we have read 4 bytes at this point, but ROOT seems to think its 3
                remaining = fByteCount - 3
        return cls(fByteCount, fVersion, fClassRef, remaining), buffer
