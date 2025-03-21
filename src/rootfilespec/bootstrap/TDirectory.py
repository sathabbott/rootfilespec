from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)
from .TKey import DICTIONARY, TKey
from .TUUID import TUUID
from .util import fDatime_to_datetime

"""
TODO: TDirectory for ROOT 3.02.06

Format of a TDirectory record in release 3.02.06. It is never compressed.
       0->0  Modified  = True if directory has been modified                           TDirectory::fModified
       1->1  Writable = True if directory is writable                                  TDirectory::fWriteable
       2->5  DatimeC   = Date and time when directory was created                      TDirectory::fDatimeC
                       | (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second
       6->9  DatimeM   = Date and time when directory was last modified                TDirectory::fDatimeM
                       | (year-1995)<<26|month<<22|day<<17|hour<<12|minute<<6|second
      10->13 NbytesKeys= Number of bytes in the associated KeysList record             TDirectory::fNbyteskeys
      14->17 NbytesName= Number of bytes in TKey+TNamed at creation                    TDirectory::fNbytesName
      18->21 SeekDir   = Byte offset of directory record in file                       TDirectory::fSeekDir
      22->25 SeekParent= Byte offset of parent directory record in file                TDirectory::fSeekParent
      26->29 SeekKeys  = Byte offset of associated KeysList record in file             TDirectory::fSeekKeys
"""


@structify(big_endian=True)
@dataclass
class TDirectory_header_v622(StructClass):
    """Format of a TDirectory record in release 6.22.06. It is never compressed.

    Header information from https://root.cern/doc/master/tdirectory.html

    Attributes:
        fVersion (int): TDirectory class version identifier
        fDatimeC (int): Date and time when directory was created
        fDatimeM (int): Date and time when directory was last modified
        fNbytesKeys (int): Number of bytes in the associated KeysList record
        fNbytesName (int): Number of bytes in TKey+TNamed at creation
    """

    fVersion: int = sfield("h")
    fDatimeC: int = sfield("I")
    fDatimeM: int = sfield("I")
    fNbytesKeys: int = sfield("i")
    fNbytesName: int = sfield("i")

    def create_time(self):
        """Date and time when directory was created"""
        return fDatime_to_datetime(self.fDatimeC)

    def modify_time(self):
        """Date and time when directory was last modified"""
        return fDatime_to_datetime(self.fDatimeM)


@dataclass
class TDirectory(ROOTSerializable):
    """TDirectory object.
    Binary Spec (the DATA section): https://root.cern.ch/doc/master/tdirectory.html

    Attributes:
        header (TDirectory_header_v622): TDirectory header information
        fSeekDir (int): Byte offset of directory record in file
        fSeekParent (int): Byte offset of parent directory record in file
        fSeekKeys (int): Byte offset of associated KeysList record in file
        fUUID (TUUID): Universally Unique Identifier
    """

    # Fields for a TDirectory
    header: TDirectory_header_v622
    fSeekDir: int
    fSeekParent: int
    fSeekKeys: int
    fUUID: TUUID

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\t\tReading TDirectory;\033[0m {buffer.info()}")
        header, buffer = TDirectory_header_v622.read(buffer)
        # print(f"\t\t\t{header}")
        if header.fVersion < 1000:
            (fSeekDir, fSeekParent, fSeekKeys), buffer = buffer.unpack(">iii")
        else:
            (fSeekDir, fSeekParent, fSeekKeys), buffer = buffer.unpack(">qqq")
        fUUID, buffer = TUUID.read(buffer)
        if header.fVersion < 1000:
            # Extra space to allow seeks to become 64 bit without moving this header
            buffer = buffer[12:]
        print("\033[1;32m\t\tDone reading TDirectory\n\033[0m")
        return cls(header, fSeekDir, fSeekParent, fSeekKeys, fUUID), buffer

    def get_KeyList(self, fetch_data: DataFetcher) -> TKeyList:
        # The TKeyList for a TDirectory contains all the (visible) TKeys
        #   For RNTuples, it will only contain the RNTuple Anchor TKey(s)
        # Binary Spec: https://root.cern.ch/doc/master/keyslist.html
        buffer = fetch_data(
            self.fSeekKeys, self.header.fNbytesName + self.header.fNbytesKeys
        )
        # abbott: For TKeyList, should just need fNbytesKeys? unless there is some TNamed thing later. (don't understand fNbytesName)
        #           need to test on different TKeyList cases to understand
        key, _ = TKey.read(buffer)
        if key.fSeekKey != self.fSeekKeys:
            msg = f"TDirectory.read_keylist: fSeekKey mismatch {key.fSeekKey} != {self.fSeekKeys}"
            raise ValueError(msg)
        if key.fSeekPdir != self.fSeekDir:
            msg = f"TDirectory.read_keylist: fSeekPdir mismatch {key.fSeekPdir} != {self.fSeekDir}"
            raise ValueError(msg)

        # abbott: Why another fetch_cached here? (why subtract fSeekKeys?)
        #  figured it out, leaving answer here in case i forget:
        #      buffer was started above with fetch_data(self.fSeekKeys ...). so the buffer starts at fSeekKeys.
        #      so, for fetch_cached, we need to subtract fSeekKeys to get the correct seek position in the buffer
        #     bc when an argument is passed to seek in fetch_cached, it will be absolute position in the file,
        #   but we need the relative position in the buffer. thus, we subtract fSeekKeys.
        def fetch_cached(seek: int, size: int):
            seek -= self.fSeekKeys
            if seek + size <= buffer.__len__():
                return buffer[seek : seek + size]
            msg = f"TDirectory.read_keylist: fetch_cached: {seek=} {size=} out of range"
            raise ValueError(msg)

        return key.read_object(fetch_cached, objtype=TKeyList)  # type: ignore[no-any-return]


DICTIONARY[b"TDirectory"] = TDirectory


@dataclass
class TKeyList(ROOTSerializable, Mapping[str, TKey]):
    # The TKeyList for a TDirectory contains all the (visible) TKeys
    #   For RNTuples, it will only contain the RNTuple Anchor TKey(s)
    # Binary Spec: https://root.cern.ch/doc/master/keyslist.html

    # Fields for a TKeyList
    fKeys: list[TKey]
    # padding: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\tReading TKeyList;\033[0m {buffer.info()}")

        (nKeys,), buffer = buffer.unpack(">I")
        keys: list[TKey] = []
        while len(keys) < nKeys:
            key, buffer = TKey.read(buffer)
            keys.append(key)
        # abbott: this code crashes the reader in my use case, but fixes a bug in Nick's use case
        #          My use case: TKeyList has one entry, a short key
        #          Nick's use case: TKeyList has multiple entries, 1 short key + 4-5 long keys
        # # suspicion: there will be 8*nshort trailing bytes
        # # corresponding to padding in case seeks need to be 64 bit
        # npad = 8 * sum(1 for k in keys if k.is_short())
        # padding, buffer = buffer.consume(npad)
        print("\033[1;32m\tDone reading TKeyList\n\033[0m")
        # return cls(keys, padding), buffer
        return cls(keys), buffer

    def __len__(self):
        return len(self.fKeys)

    def __iter__(self):
        return (key.fName.fString.decode("ascii") for key in self.fKeys)

    def __getitem__(self, key: str):
        bkey = key.encode("ascii")
        matches = [k for k in self.fKeys if k.fName.fString == bkey]
        if not len(matches):
            raise KeyError(key)
        return max(matches, key=lambda k: k.header.fCycle)
