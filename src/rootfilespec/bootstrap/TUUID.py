from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from ..structutil import ReadContext, ROOTSerializable, read_as


@dataclass
class TUUID(ROOTSerializable):
    fVersion: int
    fUUID: UUID

    @classmethod
    def read(cls, buffer: memoryview, _: ReadContext):
        (fVersion,), buffer = read_as(">h", buffer)
        uuid = UUID(bytes=buffer[:16].tobytes())
        return cls(fVersion, uuid), buffer[16:]
