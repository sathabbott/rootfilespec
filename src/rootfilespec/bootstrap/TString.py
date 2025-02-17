from __future__ import annotations

from dataclasses import dataclass

from ..structutil import ReadContext, ROOTSerializable, read_as


@dataclass
class TString(ROOTSerializable):
    fString: bytes

    @classmethod
    def read(cls, buffer: memoryview, _: ReadContext):
        (length,), buffer = read_as(">B", buffer)
        if length == 255:
            (length,), buffer = read_as(">i", buffer)
        data = buffer[:length].tobytes()
        return cls(data), buffer[length:]
