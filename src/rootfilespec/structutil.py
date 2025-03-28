from __future__ import annotations

import dataclasses
import struct
from functools import partial
from inspect import get_annotations  # type: ignore[attr-defined]
from typing import (
    Annotated,
    Any,
    Callable,
    TypeVar,
    get_args,
    get_origin,
    get_type_hints,
)

from typing_extensions import dataclass_transform  # in typing for Python 3.11+


@dataclasses.dataclass
class ReadBuffer:
    """A ReadBuffer is a memoryview that keeps track of the absolute and relative
    positions of the data it contains.

    Attributes:
        data (memoryview): The data contained in the buffer.
        abspos (int | None): The absolute position of the buffer in the file.
            If the buffer was created from a compressed buffer, this will be None.
        relpos (int): The relative position of the buffer from the start of the TKey.
        local_refs (dict[int, bytes], optional): A dictionary of local references that may
            be found in the buffer (for use reading StreamHeader data)
    """

    data: memoryview
    abspos: int | None
    relpos: int
    local_refs: dict[int, bytes] = dataclasses.field(default_factory=dict)

    def __getitem__(self, key: slice) -> ReadBuffer:
        """Get a slice of the buffer."""
        start: int = key.start or 0
        if start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.abspos + start if self.abspos is not None else None,
            self.relpos + start,
            self.local_refs,
        )

    def __len__(self) -> int:
        """Get the length of the buffer."""
        return len(self.data)

    def __repr__(self) -> str:
        """Get a string representation of the buffer."""
        return (
            f"ReadBuffer size {len(self.data)} at abspos={self.abspos}, relpos={self.relpos}"
            "\n  local_refs: "
            + "".join(f"\n    {k}: {v!r}" for k, v in self.local_refs.items())
            + "\n  data[:0x100]: "
            + "".join(
                f"\n    0x{i:03x} | "
                + self.data[i : i + 16].hex(sep=" ")
                + " | "
                + "".join(
                    chr(c) if 32 <= c < 127 else "." for c in self.data[i : i + 16]
                )
                for i in range(0, min(256, len(self)), 16)
            )
        )

    def __bool__(self) -> bool:
        return bool(self.data)

    def unpack(self, fmt: str | struct.Struct) -> tuple[tuple[Any, ...], ReadBuffer]:
        """Unpack the buffer according to the given format."""
        if isinstance(fmt, struct.Struct):
            return fmt.unpack(self.data[: fmt.size]), self[fmt.size :]
        size = struct.calcsize(fmt)
        return struct.unpack(fmt, self.data[:size]), self[size:]

    def consume(self, size: int) -> tuple[bytes, ReadBuffer]:
        """Consume the given number of bytes from the buffer."""
        return bytes(self.data[:size]), self[size:]


DataFetcher = Callable[[int, int], ReadBuffer]
ReadMethod = Callable[[ReadBuffer], tuple[Any, ReadBuffer]]
OutType = TypeVar("OutType")


class Fmt:
    """A class to hold the format of a field."""

    def __init__(self, fmt: str):
        self.fmt = fmt

    def __repr__(self) -> str:
        return f"Fmt({self.fmt})"

    def read_as(
        self, outtype: type[OutType], buffer: ReadBuffer
    ) -> tuple[OutType, ReadBuffer]:
        args, buffer = buffer.unpack(self.fmt)
        return outtype(*args), buffer


T = TypeVar("T", bound="ROOTSerializable")


@dataclasses.dataclass
class ROOTSerializable:
    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        members, buffer = cls.read_members(buffer)
        return cls(*members), buffer

    @classmethod
    def read_members(cls, buffer: ReadBuffer) -> tuple[tuple[Any, ...], ReadBuffer]:
        msg = "Unimplemented method: {cls.__name__}.read_members"
        raise NotImplementedError(msg)


@dataclass_transform()
def serializable(cls: type[T]) -> type[T]:
    """A decorator to add a read_members method to a class that reads its fields from a buffer.

    The class must have type hints for its fields, and the fields must be of types that
    either have a read method or are subscripted with a Fmt object.
    """
    cls = dataclasses.dataclass(cls)

    # if the class already has a read_members method, don't overwrite it
    readmethod = getattr(cls, "read_members", None)
    if (
        readmethod
        and getattr(readmethod, "__qualname__", None)
        == f"{cls.__qualname__}.read_members"
    ):
        return cls

    constructors: list[ReadMethod] = []
    namespace = get_type_hints(cls, include_extras=True)
    for field in get_annotations(cls):
        ftype = namespace[field]
        if isinstance(ftype, type) and issubclass(ftype, ROOTSerializable):
            constructors.append(ftype.read)
        elif origin := get_origin(ftype):
            if origin is Annotated:
                ftype, *annotations = get_args(ftype)
                fmt = next((a for a in annotations if isinstance(a, Fmt)), None)
                if fmt:
                    # TODO: potential optimization: consecutive struct fields could be read in one struct.unpack call
                    constructors.append(partial(fmt.read_as, ftype))
                else:
                    msg = f"Cannot read field {field} of type {ftype} with annotations {annotations}"
                    raise NotImplementedError(msg)
            else:
                msg = f"Cannot read field {field} of subscripted type {ftype} with origin {origin}"
                raise NotImplementedError(msg)
        else:
            msg = f"Cannot read field {field} of type {ftype}"
            raise NotImplementedError(msg)

    @classmethod  # type: ignore[misc]
    def read_members(
        _: type[T], buffer: ReadBuffer
    ) -> tuple[tuple[Any, ...], ReadBuffer]:
        args = []
        for constructor in constructors:
            arg, buffer = constructor(buffer)
            args.append(arg)
        return tuple(args), buffer

    cls.read_members = read_members  # type: ignore[assignment]
    return cls
