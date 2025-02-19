from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum

from ..structutil import (
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)
from .streamedobject import StreamHeader
from .TKey import DICTIONARY
from .TString import TString


class fBits(IntEnum):
    """Bits for TObject class.

    Relevant bits for ROOTIO are:
        kCanDelete - if object in a list can be deleted.
        kMustCleanup - if other objects may need to be deleted when this one is.
        kIsReferenced - if object is referenced by pointer to persistent object.
        kZombie - if object ctor succeeded but object shouldn't be used
        kIsOnHeap - if object is on Heap.
        kNotDeleted - if object has not been deleted.
    """

    kCanDelete = 0x00000001
    kIsOnHeap = 0x01000000
    kIsReferenced = 0x00000010
    kMustCleanup = 0x00000008
    kNotDeleted = 0x02000000
    kZombie = 0x00002000


@structify(big_endian=True)
@dataclass
class TObject_header(StructClass):
    """Header data for TObject class.

    Attributes:
        fVersion (int): Version of TObject Class
        fUniqueID (int): Unique ID of object.
        fBits (int): A 32 bit mask containing status bits for the object.
            See fBits enum for details.
    """

    fVersion: int = sfield("h")
    fUniqueID: int = sfield("i")
    fBits: int = sfield("i")

    def is_referenced(self) -> bool:
        return bool(self.fBits & fBits.kIsReferenced)


@dataclass
class TObject(ROOTSerializable):
    """Format for TObject class.

    Reference: https://root.cern/doc/master/tobject.html

    Attributes:
        sheader (StreamedObject): Streamed object header.
        header (TObject_header): Header data for TObject class.
        pidf (int): An identifier of the TProcessID record for the process that wrote the
            object. This identifier is an unsigned short. The relevant record
            has a name that is the string "ProcessID" concatenated with the ASCII
            decimal representation of "pidf" (no leading zeros). 0 is a valid pidf.
            Only present if the object is referenced by a pointer to persistent object.
    """

    sheader: StreamHeader
    header: TObject_header
    pidf: int | None

    @classmethod
    def read(cls, buffer: ReadBuffer):
        sheader, buffer = StreamHeader.read(buffer)
        header, buffer = TObject_header.read(buffer)
        pidf = None
        if header.is_referenced():
            (pidf,), buffer = buffer.unpack(">H")
        return cls(sheader, header, pidf), buffer


DICTIONARY[b"TObject"] = TObject


@dataclass
class TNamed(ROOTSerializable):
    """Format for TNamed class.

    Attributes:
        b_object (TObject): TObject base class.
        fName (TString): Name of the object.
        fTitle (TString): Title of the object.
    """

    sheader: StreamHeader
    b_object: TObject
    fName: TString
    fTitle: TString


DICTIONARY[b"TNamed"] = TNamed
