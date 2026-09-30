import dataclasses
import operator
from typing import Literal, get_args

from rootfilespec.serializable import Members, MemberSerDe, ReadBuffer, ROOTSerializable


@dataclasses.dataclass
class _FmtReader:
    fname: str
    fmt: str
    outtype: type

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        if self.fmt in ("float16", "charstar"):
            msg = f"Unimplemented format {self.fmt}"
            raise NotImplementedError(msg)
        tup, buffer = buffer.unpack(self.fmt)
        members[self.fname] = self.outtype(*tup)
        return members, buffer


@dataclasses.dataclass
class Fmt(MemberSerDe):
    """A class to hold the format of a field."""

    fmt: str

    def build_reader(self, fname: str, ftype: type):
        return _FmtReader(fname, self.fmt, ftype)


StringEncoding = Literal["RNTuple", "TString", "std::string", "charstar"]
"""The on-disk string encodings (root-io-spec Conventions §5)

- ``"RNTuple"``: a u32 little-endian length, then that many bytes.
- ``"TString"``: the counted string, one length byte, or 255 then a u32
  big-endian length (§5.1). A ``TString`` data member and a ``std::string``
  element of a collection use it.
- ``"std::string"``: a ``std::string`` data member, the counted string inside a
  byte count and a version word (§5.3).
- ``"charstar"``: a ``char*`` member (``kCharStar``), an i32 big-endian length,
  then that many bytes, with no terminator and no 255 escape (§5.4). A length of
  zero or less means no bytes follow, so a null pointer reads as ``b""``.
"""

_kByteCountMask = 0x40000000


def read_string(
    buffer: ReadBuffer, encoding: StringEncoding
) -> tuple[bytes, ReadBuffer]:
    """Read one string in the given encoding, as plain ``bytes``

    The bytes are not decoded: ROOT strings are uninterpreted bytes, and a reader
    should not assume an encoding (root-io-spec Conventions §5.1).
    """
    if encoding == "RNTuple":
        (length,), buffer = buffer.unpack("<I")
    elif encoding == "TString":
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">I")
    elif encoding == "std::string":
        (bytecount, _version), buffer = buffer.unpack(">IH")
        if not bytecount & _kByteCountMask:
            msg = f"std::string member without a byte count: {bytecount:#010x}"
            raise ValueError(msg)
        start = buffer.relpos
        data, buffer = read_string(buffer, "TString")
        if buffer.relpos - start != (bytecount & ~_kByteCountMask) - 2:
            msg = f"std::string byte count {bytecount & ~_kByteCountMask} does not match its {len(data)}-byte string"
            raise ValueError(msg)
        return data, buffer
    else:  # "charstar", checked by ROOTString
        (length,), buffer = buffer.unpack(">i")
        length = max(length, 0)
    return buffer.consume(length)


@dataclasses.dataclass
class _ROOTStringReader:
    fname: str
    encoding: StringEncoding

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        members[self.fname], buffer = read_string(buffer, self.encoding)
        return members, buffer


@dataclasses.dataclass
class ROOTString(MemberSerDe):
    """A string member, read as plain ``bytes``: ``Annotated[bytes, ROOTString(...)]``

    ``encoding`` is one of the on-disk encodings of :data:`StringEncoding`.
    """

    encoding: StringEncoding

    def __post_init__(self) -> None:
        if self.encoding not in get_args(StringEncoding):
            msg = f"Unknown string encoding {self.encoding!r}"
            raise ValueError(msg)

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        return _ROOTStringReader(fname, self.encoding)


@dataclasses.dataclass
class _OptionalFieldReader:
    """A class to read an optional field from a buffer."""

    fname: str
    fmt: str
    flagname: str
    operation: str
    flagvalue: int
    ftype: type

    def __call__(
        self, members: Members, buffer: ReadBuffer
    ) -> tuple[Members, ReadBuffer]:
        flag = members[self.flagname]
        ops = {
            "&": lambda a, b: a & b,
            "==": operator.eq,
            "!=": operator.ne,
            ">=": operator.ge,
            "<=": operator.le,
            ">": operator.gt,
            "<": operator.lt,
        }
        op_func = ops.get(self.operation)
        if op_func is None:
            msg = f"Unsupported operation: {self.operation}. Supported operations: {', '.join(ops.keys())}"
            raise ValueError(msg)
        if op_func(flag, self.flagvalue):
            if self.fmt == "class":
                if not issubclass(self.ftype, ROOTSerializable):
                    msg = f"Expected ftype to be a subclass of ROOTSerializable, got {self.ftype}"
                    raise TypeError(msg)
                members[self.fname], buffer = self.ftype.read(buffer)
            else:
                tup, buffer = buffer.unpack(self.fmt)
                members[self.fname] = self.ftype(*tup)
        else:
            members[self.fname] = None
        return members, buffer


@dataclasses.dataclass
class OptionalField(MemberSerDe):
    """A class to hold an optional field format.
    Optional fields are fields that may or may not be present in the data.
    They are only read if the value of the flag from flagname matches flagvalue
    according to the specified operation."""

    fmt: str
    flagname: str
    operation: str
    flagvalue: int

    def build_reader(self, fname: str, ftype: type):
        ftype, _ = get_args(ftype)  # Get the type inside Optional
        return _OptionalFieldReader(
            fname, self.fmt, self.flagname, self.operation, self.flagvalue, ftype
        )


@dataclasses.dataclass
class StdBitset(MemberSerDe):
    """A class to hold a std::bitset of a given size."""

    size: int

    def build_reader(self, fname: str, ftype: type):  # noqa: ARG002
        # fmt = ">I"
        # if self.size > 32:
        #     fmt = ">Q"
        # if self.size > 64:
        #     msg = f"Unimplemented size {self.size}"
        #     raise NotImplementedError(msg)

        # return _FmtReader(fname, fmt, ftype)
        def reader(members: Members, buffer: ReadBuffer) -> tuple[Members, ReadBuffer]:
            msg = "StdBitset reader not implemented"
            raise NotImplementedError(msg)

        return reader
