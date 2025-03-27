from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Annotated

from rootfilespec.bootstrap.streamedobject import StreamHeader
from rootfilespec.bootstrap.TKey import DICTIONARY
from rootfilespec.bootstrap.TList import TObjArray
from rootfilespec.bootstrap.TObject import TNamed
from rootfilespec.bootstrap.TString import TString
from rootfilespec.structutil import (
    Fmt,
    ReadBuffer,
    ROOTSerializable,
    serializable,
)


@serializable
class TStreamerInfo(ROOTSerializable):
    sheader: StreamHeader
    b_named: TNamed
    fCheckSum: Annotated[int, Fmt(">i")]
    fClassVersion: Annotated[int, Fmt(">i")]
    fObjects: TObjArray


DICTIONARY[b"TStreamerInfo"] = TStreamerInfo


class ElementType(IntEnum):
    """Element type codes.

    Built in types:
    1:char, 2:short, 3:int, 4:long, 5:float, 8:double
    11, 12, 13, 14:unsigned char, short, int, long respectively
    6: an array dimension (counter)
    15: bit mask (used for fBits field)

    Pointers to built in types:
    40 + fType of built in type (e.g. 43: pointer to int)

    Objects:
    65:TString, 66:TObject, 67:TNamed
    0: base class (other than TObject or TNamed)
    61: object data member derived from TObject (other than TObject or TNamed)
    62: object data member not derived from TObject
    63: pointer to object derived from TObject (pointer can't be null)
    64: pointer to object derived from TObject (pointer may be null)
    501: pointer to an array of objects
    500: an STL string or container

    Arrays:
    20 + fType of array element (e.g. 23: array of int)
    """

    kChar = 1
    kShort = 2
    kInt = 3
    kLong = 4
    kFloat = 5
    kDouble = 8
    kUChar = 11
    kUShort = 12
    kUInt = 13
    kULong = 14
    kLong64 = 16
    kBool = 18
    kArrayDim = 6
    kBitMask = 15
    kPointer = 40
    kPointerChar = 41
    kPointerShort = 42
    kPointerInt = 43
    kPointerLong = 44
    kPointerFloat = 45
    kPointerDouble = 48
    kPointerUChar = 51
    kPointerUShort = 52
    kPointerUInt = 53
    kPointerULong = 54
    kPointerLong64 = 56
    kTString = 65
    kTObject = 66
    kTNamed = 67
    kBaseClass = 0
    kObjectDataMemberTObject = 61
    kObjectDataMember = 62
    kPointerTObjectNotNull = 63
    kPointerTObjectNullable = 64
    kPointerArray = 501
    kSTL = 500

    def as_fmt(self) -> str:
        fmtmap = {
            self.kChar: "Annotated[int, '>b']",
            self.kShort: "Annotated[int, '>h']",
            self.kInt: "Annotated[int, '>i']", 
            self.kLong: "Annotated[int, '>l']",
            self.kFloat: "Annotated[float, '>f']",
            self.kDouble: "Annotated[float, '>d']",
            self.kUChar: "Annotated[int, '>B']",
            self.kUShort: "Annotated[int, '>H']",
            self.kUInt: "Annotated[int, '>I']",
            self.kULong: "Annotated[int, '>L']",
            self.kLong64: "Annotated[int, '>q']",
            self.kBool: "Annotated[bool, '>?']",
            self.kBitMask: "Annotated[int, '>I']",
            self.kArrayDim: "Annotated[int, '>i']",
        }

        if self not in fmtmap:
            if self - 40 in fmtmap:
                return fmtmap[ElementType(self - 40)]
            msg = f"Cannot convert {self!r} to format character"
            raise ValueError(msg)

        return fmtmap[self]

@dataclass
class ArrayDim:
    dim0: int
    dim1: int
    dim2: int
    dim3: int
    dim4: int


@serializable
class TStreamerElement(ROOTSerializable):
    """TStreamerElement class.

    Reference: https://root.cern/doc/master/streamerinfo.html (TStreamerElement section)

    Attributes:
        fType (int): Type of data described by this TStreamerElement.
        fSize (int): Size of built in type or of pointer to built in type. 0 otherwise.
        fArrayLength (int): Size of array (0 if not array)
        fArrayDim (int): Number of dimensions of array (0 if not an array)
        fMaxIndex (int[5]): Five integers giving the array dimensions (0 if not applicable)
    """

    sheader: StreamHeader
    b_TNamed: TNamed
    fType: Annotated[ElementType, Fmt(">i")]
    fSize: Annotated[int, Fmt(">i")]
    fArrayLength: Annotated[int, Fmt(">i")]
    fArrayDim: Annotated[int, Fmt(">i")]
    fMaxIndex: Annotated[ArrayDim, Fmt("5i")]
    fTypeName: TString


DICTIONARY[b"TStreamerElement"] = TStreamerElement


@serializable
class TStreamerBase(ROOTSerializable):
    """Streamer element for a base class.

    Attributes:
        sheader (StreamHeader): Stream header.
        b_element (TStreamerElement): Base streamer element.
        fBaseVersion (int): Version of base class.
    """

    sheader: StreamHeader
    b_element: TStreamerElement
    fBaseVersion: Annotated[int, Fmt(">i")]


DICTIONARY[b"TStreamerBase"] = TStreamerBase


@serializable
class TStreamerBasicType(ROOTSerializable):
    sheader: StreamHeader
    b_element: TStreamerElement


DICTIONARY[b"TStreamerBasicType"] = TStreamerBasicType


@serializable
class TStreamerString(ROOTSerializable):
    sheader: StreamHeader
    b_element: TStreamerElement


DICTIONARY[b"TStreamerString"] = TStreamerString


@serializable
class TStreamerBasicPointer(ROOTSerializable):
    """Streamer element for a pointer to a built in type.

    Attributes:
        sheader (StreamHeader): Stream header.
        b_element (TStreamerElement): Base streamer element.
        fCountVersion (int): Version of count variable.
        fCountName (TString): Name of count variable.
        fCountClass (TString): Class of count variable.
    """

    sheader: StreamHeader
    b_element: TStreamerElement
    fCountVersion: Annotated[int, Fmt(">i")]
    fCountName: TString
    fCountClass: TString


DICTIONARY[b"TStreamerBasicPointer"] = TStreamerBasicPointer


@serializable
class TStreamerObject(ROOTSerializable):
    sheader: StreamHeader
    b_element: TStreamerElement


DICTIONARY[b"TStreamerObject"] = TStreamerObject


@serializable
class TStreamerObjectPointer(ROOTSerializable):
    sheader: StreamHeader
    b_element: TStreamerElement


DICTIONARY[b"TStreamerObjectPointer"] = TStreamerObjectPointer


@serializable
class TStreamerLoop(ROOTSerializable):
    """Loop streamer element.

    Attributes:
        sheader (StreamHeader): Stream header.
        b_element (TStreamerElement): Base streamer element.
        fCountVersion (int): Version of count variable.
        fCountName (TString): Name of count variable.
        fCountClass (TString): Class of count variable.
    """

    sheader: StreamHeader
    b_element: TStreamerElement
    fCountVersion: Annotated[int, Fmt(">i")]
    fCountName: TString
    fCountClass: TString


DICTIONARY[b"TStreamerLoop"] = TStreamerLoop


@serializable
class TStreamerObjectAny(ROOTSerializable):
    sheader: StreamHeader
    b_element: TStreamerElement


DICTIONARY[b"TStreamerObjectAny"] = TStreamerObjectAny


@serializable
class TStreamerSTL(ROOTSerializable):
    """STL container streamer element.

    Attributes:
        sheader (StreamHeader): Stream header.
        b_element (TStreamerElement): Base streamer element.
        fSTLtype (int): Type of STL container.
            1:vector, 2:list, 3:deque, 4:map, 5:set, 6:multimap, 7:multiset
        fCType (int): Type contained in STL container.
            Same values as for fType above, with one addition: 365:STL string

    """

    sheader: StreamHeader
    b_element: TStreamerElement
    fSTLtype: Annotated[int, Fmt(">i")]
    fCType: Annotated[int, Fmt(">i")]


DICTIONARY[b"TStreamerSTL"] = TStreamerSTL


@serializable
class TStreamerSTLString(ROOTSerializable):
    """STL string streamer element.

    Attributes:
        sheader (StreamHeader): Stream header.
        b_element (TStreamerElement): Base streamer element.
        uninterpreted (bytes): Uninterpreted data.
            TODO: According to ROOT docs, there should not be any extra data here
    """

    sheader: StreamHeader
    b_element: TStreamerElement
    uninterpreted: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        initial_position = buffer.relpos
        sheader, buffer = StreamHeader.read(buffer)
        b_element, buffer = TStreamerElement.read(buffer)
        expected_position = initial_position + sheader.fByteCount + 4
        uninterpreted, buffer = buffer.consume(expected_position - buffer.relpos)
        return cls(sheader, b_element, uninterpreted), buffer


# the lower case "string" is intentional
DICTIONARY[b"TStreamerSTLstring"] = TStreamerSTLString
