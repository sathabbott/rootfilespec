from __future__ import annotations

import dataclasses
import struct
from typing import TypeVar, get_type_hints


@dataclasses.dataclass
class ReadContext:
    """Context for all ROOTSerializable object read methods.

    This should stay sparse and only contain information that is needed

    Attributes:
        key_length (int): Length of the key. Some objects use local offsets
            relative to the start of the key+data buffer.
    """

    key_length: int


T = TypeVar("T", bound="ROOTSerializable")


class ROOTSerializable:
    @classmethod
    def read(
        cls: type[T], buffer: memoryview, context: ReadContext
    ) -> tuple[T, memoryview]:
        args = []
        namespace = get_type_hints(cls)
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            ftype = namespace[field.name]
            if issubclass(ftype, ROOTSerializable):
                arg, buffer = ftype.read(buffer, context)
            else:
                msg = f"Cannot read field {field.name} of type {ftype}"
                raise NotImplementedError(msg)
            args.append(arg)
        return cls(*args), buffer


S = TypeVar("S", bound="StructClass")


class StructClass(ROOTSerializable):
    _struct: struct.Struct

    @classmethod
    def size(cls) -> int:
        return cls._struct.size

    @classmethod
    def read(cls: type[S], buffer: memoryview, _: ReadContext) -> tuple[S, memoryview]:
        fmt = cls._struct
        args = fmt.unpack(buffer[: fmt.size])
        return cls(*args), buffer[fmt.size :]


def structify(big_endian: bool):
    """A decorator to add a precompiled struct.Struct object to a StructClass."""

    endianness = ">" if big_endian else "<"

    def decorator(cls):
        fmt = "".join(f.metadata["format"] for f in dataclasses.fields(cls))
        cls._struct = struct.Struct(endianness + fmt)
        return cls

    return decorator


def sfield(fmt: str):
    """A dataclass field that has a struct format."""
    return dataclasses.field(metadata={"format": fmt})


def read_as(fmt: str, buffer: memoryview):
    """Read a struct from a buffer."""
    size = struct.calcsize(fmt)
    return struct.unpack(fmt, buffer[:size]), buffer[size:]
