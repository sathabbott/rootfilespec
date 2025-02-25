from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)
from .TDirectory import TDirectory
from .TKey import DICTIONARY, TKey
from .TList import TList
from .TString import TString
from .TUUID import TUUID


@dataclass(order=True)
class VersionInfo(ROOTSerializable):
    major: int
    minor: int
    cycle: int
    large: bool = False  # File is larger than 32bit file limit (2GB)

    @classmethod
    def read(cls, buffer: ReadBuffer):
        (version,), buffer = buffer.unpack(">I")
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

    fBEGIN: int = sfield("I")
    fEND: int = sfield("I")
    fSeekFree: int = sfield("I")
    fNbytesFree: int = sfield("I")
    nfree: int = sfield("I")
    fNbytesName: int = sfield("I")
    fUnits: int = sfield("B")
    fCompress: int = sfield("I")
    fSeekInfo: int = sfield("I")
    fNbytesInfo: int = sfield("I")


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
    """ A class representing a ROOT file.
    Binary Spec: https://root.cern.ch/doc/master/classTFile.html
        
    Attributes:
        magic (bytes): The magic number identifying the file as a ROOT file.
        fVersion (VersionInfo): The version information of the ROOT file.
        header (Union[ROOTFile_header_v302, ROOTFile_header_v622_small, ROOTFile_header_v622_large]): The header of the ROOT file.
        UUID (Union[bytes, TUUID]): The UUID of the ROOT file.
        padding (bytes): Padding bytes in the ROOT file.

    Methods:
        read(cls, buffer: ReadBuffer) -> Tuple[ROOTFile, ReadBuffer]:
            Reads a ROOT file from the given buffer and returns a ROOTFile instance and the remaining buffer.

        get_TFile(self, fetch_data: DataFetcher) -> TFile:
            Retrieves the TFile object (root directory) from the file using the provided data fetcher.

        get_StreamerInfo(self, fetch_data: DataFetcher) -> TList:
            Retrieves the StreamerInfo (list of streamers) from the file using the provided data fetcher.
    """
    
    # Fields for the ROOT file header
    magic: bytes
    fVersion: VersionInfo
    # Field for the actual header. The version determines which header to use.
    header: (
        ROOTFile_header_v302 | ROOTFile_header_v622_small | ROOTFile_header_v622_large
    )
    UUID: bytes | TUUID
    padding: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads and parses a ROOT file from the given buffer.
        Binary Spec: https://root.cern.ch/doc/master/classTFile.html
                     https://root.cern.ch/doc/master/header.html
        Args:
            buffer (ReadBuffer): The buffer containing the ROOT file data.

        Returns:
            tuple: A tuple containing the parsed ROOT file object and the remaining buffer.

        Raises:
            ValueError: If the magic number in the buffer is not 'root'.

        The function performs the following steps:
        1. Unpacks the magic number from the buffer and checks if it is 'root'.
        2. Reads the version information from the buffer.
        3. Depending on the version, reads the appropriate header and UUID from the buffer.
        4. Consumes the padding bytes from the buffer.
        5. Returns the parsed ROOT file object and the remaining buffer.
        """
        print("\033[1;96mReading TFile Header...\033[0m")
        (magic,), buffer = buffer.unpack("4s")
        if magic != b"root":
            msg = f"ROOTFile.read: magic is not 'root': {magic!r}"
            raise ValueError(msg)
        fVersion, buffer = VersionInfo.read(buffer)
        if fVersion <= VersionInfo(6, 2, 2):
            header, buffer = ROOTFile_header_v302.read(buffer)
            uuid, buffer = buffer.consume(16)
        elif not fVersion.large:
            header, buffer = ROOTFile_header_v622_small.read(buffer)  # type: ignore[assignment]
            uuid, buffer = TUUID.read(buffer)
        else:
            header, buffer = ROOTFile_header_v622_large.read(buffer)  # type: ignore[assignment]
            uuid, buffer = TUUID.read(buffer)
        padding, buffer = buffer.consume(header.fBEGIN - buffer.relpos)
        return cls(magic, fVersion, header, uuid, padding), buffer

    def get_TFile(self, fetch_data: DataFetcher) -> TFile:
        """Get the TFile object (root directory) from the file."""
        print("\033[1;96mReading TFile Key + Object...\033[0m")
        buffer = fetch_data(self.header.fBEGIN, self.header.fNbytesName)
        key, buffer = TKey.read(buffer)
        if key.fSeekKey != self.header.fBEGIN:
            msg = f"ROOTFile.read_rootkey: key.fSeekKey != self.header.fBEGIN: {key.fSeekKey} != {self.header.fBEGIN}"
            raise ValueError(msg)
        if key.fSeekPdir != 0:
            msg = f"ROOTFile.read_rootkey: key.fSeekPdir != 0: {key.fSeekPdir} != 0"
            raise ValueError(msg)
        return key.read_object(fetch_data)  # type: ignore[no-any-return]

    def get_StreamerInfo(self, fetch_data: DataFetcher) -> TList:
        buffer = fetch_data(self.header.fSeekInfo, self.header.fNbytesInfo)
        key, _ = TKey.read(buffer)
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
    """ The TFile object is a TDirectory with an extra name and title field (the first or "root" TDirectory):
        Binary Spec (the DATA section): https://root.cern.ch/doc/master/tfile.html
    
    TDirectory otherwise has its name and title in its owning TKey object (see TDirectory class).
    """

    # Header fields for the root TDirectory
    fName: TString
    fTitle: TString
    # Field for the actual root TDirectory (formatted like a normal TDirectory)
    rootdir: TDirectory

    def get_KeyList(self, fetch_data):
        return self.rootdir.get_KeyList(fetch_data)


DICTIONARY[b"TFile"] = TFile
