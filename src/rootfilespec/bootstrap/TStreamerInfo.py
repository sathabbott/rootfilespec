from __future__ import annotations

from dataclasses import dataclass

from ..structutil import ReadContext, ROOTSerializable


@dataclass
class TStreamerInfo(ROOTSerializable):
    @classmethod
    def read(
        cls, buffer: memoryview, _: ReadContext
    ) -> tuple[TStreamerInfo, memoryview]:
        msg = "TStreamerInfo.read not implemented"
        raise NotImplementedError(msg)
