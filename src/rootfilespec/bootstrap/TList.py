from __future__ import annotations

from dataclasses import dataclass

from ..structutil import ReadContext, ROOTSerializable, read_as
from .streamedobject import StreamHeader
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
    items: list[StreamHeader]

    @classmethod
    def read(cls, buffer: memoryview, context: ReadContext):
        header, buffer = TObject.read(buffer, context)
        fName, buffer = TString.read(buffer, context)
        (fN,), buffer = read_as(">i", buffer)
        items: list[StreamHeader] = []
        for _ in range(fN):
            item, buffer = StreamHeader.read(buffer, context)
            # TODO: read the object
            buffer = buffer[item.remaining :]
            items.append(item)
        return cls(header, fName, fN, items), buffer


DICTIONARY[b"TList"] = TList
