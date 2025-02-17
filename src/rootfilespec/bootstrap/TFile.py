from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    ReadContext,
    ROOTSerializable,
    StructClass,
    read_as,
    sfield,
    structify,
)
from .TDirectory import TDirectory
from .TKey import DICTIONARY, TKey
from .TList import TList
from .TString import TString
from .TUUID import TUUID


@dataclass
class VersionInfo(ROOTSerializable):
    major: int
    minor: int
    cycle: int
    large: bool = False  # File is larger than 32bit file limit (2GB)

    @classmethod
    def read(cls, buffer: memoryview, _: ReadContext):
        (version,), buffer = read_as(">i", buffer)
        return cls(
            major=version // 10_000 % 100,
            minor=version // 100 % 100,
            cycle=version % 100,
            large=version > 1_000_000,
        ), buffer


@structify(big_endian=True)
@dataclass
class ROOTFile_header_v302(StructClass):
    """
    A class representing the header structure for ROOTFile version 3.02.06

    Header information from https://root.cern/doc/master/header.html

    Attributes:
        fBEGIN (int): Byte offset of first data record (64)
        fEND (int): Pointer to first free word at the EOF
        fSeekFree (int): Byte offset of FreeSegments record
        fNbytesFree (int): Number of bytes in FreeSegments record
        nfree (int): Number of free data records
        fNbytesName (int): Number of bytes in TKey+TNamed for ROOTFile at creation
        fUnits (int): Number of bytes for file pointers (4)
        fCompress (int): Zip compression level (i.e. 0-9)
        fSeekInfo (int): Byte offset of StreamerInfo record
        fNbytesInfo (int): Number of bytes in StreamerInfo record
        fUUID (bytes): Unique identifier for the file
    """

    fBEGIN: int = sfield("i")
    fEND: int = sfield("i")
    fSeekFree: int = sfield("i")
    fNbytesFree: int = sfield("i")
    nfree: int = sfield("i")
    fNbytesName: int = sfield("i")
    fUnits: int = sfield("B")
    fCompress: int = sfield("i")
    fSeekInfo: int = sfield("i")
    fNbytesInfo: int = sfield("i")


@structify(big_endian=True)
@dataclass
class ROOTFile_header_v622_small(StructClass):
    """
    A class representing the header structure for ROOTFile version 6.22.06

    If END, SeekFree, or SeekInfo are located past the 32 bit file limit (> 2000000000)
    then these fields will be 8 instead of 4 bytes and 1000000 is added to the file format version.
    The _large variant of this class is used in that case.

    Attributes:
        fBEGIN (int): Byte offset of first data record (100)
        fEND (int): Pointer to first free word at the EOF
        fSeekFree (int): Byte offset of FreeSegments record
        fNbytesFree (int): Number of bytes in FreeSegments record
        nfree (int): Number of free data records
        fNbytesName (int): Number of bytes in TKey+TNamed for ROOTFile at creation
        fUnits (int): Number of bytes for file pointers (4 or 8)
        fCompress (int): Zip compression level (i.e. 0-9)
        fSeekInfo (int): Byte offset of StreamerInfo record
        fNbytesInfo (int): Number of bytes in StreamerInfo record
    """

    fBEGIN: int = sfield("i")
    fEND: int = sfield("i")
    fSeekFree: int = sfield("i")
    fNbytesFree: int = sfield("i")
    nfree: int = sfield("i")
    fNbytesName: int = sfield("i")
    fUnits: int = sfield("B")
    fCompress: int = sfield("i")
    fSeekInfo: int = sfield("i")
    fNbytesInfo: int = sfield("i")


@structify(big_endian=True)
@dataclass
class ROOTFile_header_v622_large(StructClass):
    __doc__ = ROOTFile_header_v622_small.__doc__

    fBEGIN: int = sfield("i")
    fEND: int = sfield("q")
    fSeekFree: int = sfield("q")
    fNbytesFree: int = sfield("i")
    nfree: int = sfield("i")
    fNbytesName: int = sfield("i")
    fUnits: int = sfield("B")
    fCompress: int = sfield("i")
    fSeekInfo: int = sfield("q")
    fNbytesInfo: int = sfield("i")


@dataclass
class ROOTFile(ROOTSerializable):
    magic: bytes
    fVersion: VersionInfo
    header: (
        ROOTFile_header_v302 | ROOTFile_header_v622_small | ROOTFile_header_v622_large
    )
    UUID: bytes | TUUID
    padding: bytes

    @classmethod
    def read(cls, buffer: memoryview, context: ReadContext):
        initial_size = len(buffer)
        (magic,), buffer = read_as("4s", buffer)
        if magic != b"root":
            msg = f"ROOTFile.read: magic is not 'root': {magic!r}"
            raise ValueError(msg)
        fVersion, buffer = VersionInfo.read(buffer, context)
        if fVersion.major <= 3 and fVersion.minor <= 0 and fVersion.cycle <= 2:
            header, buffer = ROOTFile_header_v302.read(buffer, context)
            uuid, buffer = read_as("16s", buffer)
        elif not fVersion.large:
            header, buffer = ROOTFile_header_v622_small.read(buffer, context)  # type: ignore[assignment]
            uuid, buffer = TUUID.read(buffer, context)
        else:
            header, buffer = ROOTFile_header_v622_large.read(buffer, context)  # type: ignore[assignment]
            uuid, buffer = TUUID.read(buffer, context)
        consumed = initial_size - len(buffer)
        padding = buffer[: header.fBEGIN - consumed].tobytes()
        buffer = buffer[header.fBEGIN - consumed :]
        return cls(magic, fVersion, header, uuid, padding), buffer

    def get_TFile(self, fetch_data) -> TFile:
        """Get the TFile object (root directory) from the file."""
        buffer = fetch_data(self.header.fBEGIN, self.header.fNbytesName)
        key, buffer = TKey.read(buffer, ReadContext(0))
        if key.fSeekKey != self.header.fBEGIN:
            msg = f"ROOTFile.read_rootkey: key.fSeekKey != self.header.fBEGIN: {key.fSeekKey} != {self.header.fBEGIN}"
            raise ValueError(msg)
        if key.fSeekPdir != 0:
            msg = f"ROOTFile.read_rootkey: key.fSeekPdir != 0: {key.fSeekPdir} != 0"
            raise ValueError(msg)
        return key.read_object(fetch_data)  # type: ignore[no-any-return]

    def get_StreamerInfo(self, fetch_data) -> TList:
        buffer = fetch_data(self.header.fSeekInfo, self.header.fNbytesInfo)
        key, _ = TKey.read(buffer, ReadContext(0))
        if key.fSeekKey != self.header.fSeekInfo:
            msg = f"ROOTFile.get_StreamerInfo: fSeekKey != fSeekInfo: {key.fSeekKey} != {self.header.fSeekInfo}"
            raise ValueError(msg)
        if key.header.fNbytes != self.header.fNbytesInfo:
            msg = f"ROOTFile.get_StreamerInfo: fNbytes != fNbytesInfo: {key.header.fNbytes} != {self.header.fNbytesInfo}"
            raise ValueError(msg)

        def fetch_cached(seek: int, size: int):
            seek -= self.header.fSeekInfo
            return buffer[seek : seek + size]

        return key.read_object(fetch_cached)  # type: ignore[no-any-return]


@dataclass
class TFile(ROOTSerializable):
    """The TFile object is a TDirectory with an extra name and title field.

    TDirectory otherwise has its name and title in its owning TKey object.
    """

    fName: TString
    fTitle: TString
    rootdir: TDirectory

    def get_KeyList(self, fetch_data):
        return self.rootdir.get_KeyList(fetch_data)


DICTIONARY[b"TFile"] = TFile
