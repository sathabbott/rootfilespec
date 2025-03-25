from __future__ import annotations

from typing import TypeVar, overload

from rootfilespec.bootstrap.compression import RCompressed
from rootfilespec.bootstrap.TString import TString
from rootfilespec.bootstrap.util import fDatime_to_datetime
from rootfilespec.structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    serializable,
    sfield,
    structify,
)

DICTIONARY: dict[bytes, type[ROOTSerializable]] = {}


@structify(big_endian=True)
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

    fNbytes: int = sfield("i")
    fVersion: int = sfield("h")
    fObjlen: int = sfield("i")
    fDatime: int = sfield("I")
    fKeylen: int = sfield("h")
    fCycle: int = sfield("h")

    def write_time(self):
        """Date and time when record was written to file"""
        return fDatime_to_datetime(self.fDatime)


ObjType = TypeVar("ObjType", bound=ROOTSerializable)


@serializable
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

    header: TKey_header
    fSeekKey: int
    fSeekPdir: int
    fClassName: TString
    fName: TString
    fTitle: TString

    @classmethod
    def read(cls, buffer: ReadBuffer):
        initial_size = len(buffer)
        header, buffer = TKey_header.read(buffer)
        if header.fVersion < 1000:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">ii")
        else:
            (fSeekKey, fSeekPdir), buffer = buffer.unpack(">qq")
        fClassName, buffer = TString.read(buffer)
        fName, buffer = TString.read(buffer)
        fTitle, buffer = TString.read(buffer)
        if len(buffer) != initial_size - header.fKeylen:
            raise ValueError("TKey.read: buffer size mismatch")  # noqa: EM101
        return cls(header, fSeekKey, fSeekPdir, fClassName, fName, fTitle), buffer

    def is_short(self) -> bool:
        """Return if the key is short (i.e. the seeks are 32 bit)"""
        return self.header.fVersion < 1000

    @overload
    def read_object(self, fetch_data: DataFetcher) -> ROOTSerializable: ...

    @overload
    def read_object(
        self, fetch_data: DataFetcher, objtype: type[ObjType]
    ) -> ObjType: ...

    def read_object(
        self,
        fetch_data: DataFetcher,
        objtype: type[ObjType] | None = None,
    ) -> ObjType | ROOTSerializable:
        buffer = fetch_data(
            self.fSeekKey + self.header.fKeylen,
            self.header.fNbytes - self.header.fKeylen,
        )
        compressed = None
        if len(buffer) != self.header.fObjlen:
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
            # if self.fClassName.fString != typename:
            #     msg = f"TKey.read_object: type mismatch: expected {typename!r} but got {self.fClassName.fString!r}"
            #     raise ValueError(msg)
            obj, buffer = objtype.read(buffer)
        else:
            typename = self.fClassName.fString
            obj, buffer = DICTIONARY[typename].read(buffer)  # type: ignore[assignment]
        if buffer:
            msg = f"TKey.read_object: buffer not empty after reading object of type {typename!r}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{obj=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        return obj
