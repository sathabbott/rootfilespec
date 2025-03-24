from __future__ import annotations

from ..structutil import ReadBuffer, ROOTSerializable, build


@build
class TString(ROOTSerializable):
    fString: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(length)
        return cls(data), buffer
