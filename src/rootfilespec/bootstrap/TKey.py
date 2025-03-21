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
from .compression import RCompressed
from .TString import TString
from .util import fDatime_to_datetime

DICTIONARY: dict[bytes, type[ROOTSerializable]] = {}


@structify(big_endian=True)
@dataclass
class TKey_header(StructClass):
    """TKey header information

    Attributes:
        fNbytes (int): Number of bytes in compressed record (Tkey+data)
        fVersion (int): TKey class version identifier
        fObjlen (int): Number of bytes of uncompressed data
        fDatime (int): Date and time when record was written to file
        fKeylen (int): Number of bytes in key structure (TKey)
        fCycle (int): Cycle of key
    """

    fNbytes: int = sfield("I")
    fVersion: int = sfield("H")
    fObjlen: int = sfield("I")
    fDatime: int = sfield("I")
    fKeylen: int = sfield("H")
    fCycle: int = sfield("H")

    def write_time(self):
        """Date and time when record was written to file"""
        return fDatime_to_datetime(self.fDatime)


@dataclass
class TKey(ROOTSerializable):
    """TKey object

    Attributes:
        header (TKey_header): TKey header information
        fSeekKey (int): Byte offset of record itself (consistency check)
        fSeekPdir (int): Byte offset of parent directory record
        fClassName (TString): Object Class Name
        fName (TString): Name of the object
        fTitle (TString): Title of the object

    See https://root.cern/doc/master/classTKey.html for more information.
    """

    # Fields for a TKey
    header: TKey_header
    fSeekKey: int
    fSeekPdir: int
    fClassName: TString
    fName: TString
    fTitle: TString

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\tReading TKey;\033[0m {buffer.info()}")
        initial_size = buffer.__len__()
        header, buffer = TKey_header.read(buffer)
        # print(f"\t\t{header}")
        if header.fVersion < 1000:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">II")
        else:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">QQ")
        fClassName, buffer = TString.read(buffer)
        fName, buffer = TString.read(buffer)
        fTitle, buffer = TString.read(buffer)
        if buffer.__len__() != initial_size - header.fKeylen:
            raise ValueError("TKey.read: buffer size mismatch")  # noqa: EM101
        print("\033[1;32m\tDone reading TKey\n\033[0m")
        return cls(header, fSeekKey, fSeekPdir, fClassName, fName, fTitle), buffer

    def is_short(self) -> bool:
        """Return if the key is short (i.e. the seeks are 32 bit)"""
        return self.header.fVersion < 1000

    def read_object(
        self,
        fetch_data: DataFetcher,
        objtype: type[ROOTSerializable] | None = None,
    ) -> ROOTSerializable:
        # for k, v in DICTIONARY.items(): print(f"{k=}, {v=}")
        buffer = fetch_data(
            self.fSeekKey
            + self.header.fKeylen,  # Points to the start of the object data
            self.header.fNbytes - self.header.fKeylen,  # The size of the object data
        )
        print(f"\033[1;36m\tReading TObject;\033[0m {buffer.info()}")
        compressed = None
        # fObjlen is the number of bytes of uncompressed data
        # The length of the buffer is the number of bytes of compressed data
        if buffer.__len__() != self.header.fObjlen:
            # This is a compressed object
            compressed, buffer = RCompressed.read(buffer)
            if compressed.header.uncompressed_size() != self.header.fObjlen:
                msg = "TKey.read_object: uncompressed size mismatch. "
                msg += (
                    f"{compressed.header.uncompressed_size()} != {self.header.fObjlen}"
                )
                msg += "\nThis might be expected for very large TBaskets"

                raise ValueError(msg)
            if buffer:
                msg = f"TKey.read_object: buffer not empty after reading compressed object. {buffer=}"
                raise ValueError(msg)
            buffer = ReadBuffer(
                compressed.decompress(),
                abspos=None,
                relpos=self.header.fKeylen,
            )
        if objtype is not None:
            typename = objtype.__name__.encode("ascii")
            obj, buffer = objtype.read(buffer)
        else:
            typename = self.fClassName.fString
            obj, buffer = DICTIONARY[typename].read(buffer)
        if buffer:
            msg = f"TKey.read_object: buffer not empty after reading object of type {typename!r}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{obj=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        print(
            f"\033[1;32m\tDone reading TObject of type {typename}; \033[0m {buffer.info()}"
        )
        return obj
