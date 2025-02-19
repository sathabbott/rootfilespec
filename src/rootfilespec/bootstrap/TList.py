from __future__ import annotations

from dataclasses import dataclass

from ..structutil import ReadBuffer, ROOTSerializable
from .streamedobject import StreamHeader, read_streamed_item
from .TKey import DICTIONARY
from .TObject import TObject
from .TString import TString


@dataclass
class TList(ROOTSerializable):
    """Format for TList class.

    Reference: https://root.cern/doc/master/streamerinfo.html (TList section)

    Attributes:
        header (TObject): TObject header.
        fName (TString): Name of the list.
        fN (int): Number of objects in the list.
        items (list[TObject]): List of objects.
    """

    header: TObject
    fName: TString
    fN: int
    items: list[ROOTSerializable]

    @classmethod
    def read(cls, buffer: ReadBuffer):
        header, buffer = TObject.read(buffer)
        if header.header.fVersion == 1 << 14 and header.header.fBits == 1 << 16:
            # This looks like schema evolution data
            # print(f"Suspicious TList header: {header}")
            # print(f"Buffer: {buffer}")
            junk, buffer = buffer.consume(len(buffer) - 1)
            return cls(header, TString(junk), 0, []), buffer
        fName, buffer = TString.read(buffer)
        (fN,), buffer = buffer.unpack(">i")
        items: list[ROOTSerializable] = []
        for _ in range(fN):
            item, buffer = read_streamed_item(buffer)
            # No idea why there is a null pad byte here
            pad, buffer = buffer.consume(1)
            if pad != b"\x00":
                msg = f"Expected null pad byte but got {pad!r}"
                raise ValueError(msg)
            items.append(item)
        return cls(header, fName, fN, items), buffer


DICTIONARY[b"TList"] = TList


@dataclass
class TObjArray(ROOTSerializable):
    sheader: StreamHeader
    b_object: TObject
    fName: TString
    nObjects: int
    fLowerBound: int
    objects: list[ROOTSerializable]

    @classmethod
    def read(cls, buffer: ReadBuffer):
        sheader, buffer = StreamHeader.read(buffer)
        b_object, buffer = TObject.read(buffer)
        fName, buffer = TString.read(buffer)
        (nObjects, fLowerBound), buffer = buffer.unpack(">ii")
        objects: list[ROOTSerializable] = []
        for _ in range(nObjects):
            item, buffer = read_streamed_item(buffer)
            objects.append(item)
        return cls(sheader, b_object, fName, nObjects, fLowerBound, objects), buffer


DICTIONARY[b"TObjArray"] = TObjArray
