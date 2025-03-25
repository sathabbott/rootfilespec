from __future__ import annotations

from rootfilespec.structutil import ReadBuffer, ROOTSerializable, serializable


@serializable
class TString(ROOTSerializable):
    fString: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(length)
        return cls(data), buffer
