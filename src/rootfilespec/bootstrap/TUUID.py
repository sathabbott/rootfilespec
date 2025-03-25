from __future__ import annotations

from uuid import UUID

from ..structutil import ReadBuffer, ROOTSerializable, serializable


@serializable
class TUUID(ROOTSerializable):
    fVersion: int
    fUUID: UUID

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (fVersion,), buffer = buffer.unpack(">h")
        data, buffer = buffer.consume(16)
        uuid = UUID(bytes=data)
        return cls(fVersion, uuid), buffer
