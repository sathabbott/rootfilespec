from __future__ import annotations

import dataclasses
import struct
from typing import Any, Callable, TypeVar, get_type_hints


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
        if key.start > len(self.data):
            msg = f"Cannot get slice {key} from buffer of length {len(self.data)}"
            raise IndexError(msg)
        return ReadBuffer(
            self.data[key],
            self.abspos + key.start if self.abspos is not None else None,
            self.relpos + key.start,
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
                for i in range(0, 256, 16)
            )
        )

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

T = TypeVar("T", bound="ROOTSerializable")


class ROOTSerializable:
    """
    A base class for objects that can be serialized and deserialized from a buffer.

    Methods
    -------
    read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        Reads an instance of the class from the provided buffer.

    Parameters
    ----------
    cls : type[T]
        The class type to be read from the buffer.
    buffer : ReadBuffer
        The buffer from which the class instance will be read.

    Returns
    -------
    tuple[T, ReadBuffer]
        A tuple containing the deserialized class instance and the remaining buffer.

    Raises
    ------
    NotImplementedError
        If a field's type is not a subclass of ROOTSerializable.
    """
    @classmethod
    def read(cls: type[T], buffer: ReadBuffer) -> tuple[T, ReadBuffer]:
        args = []
        namespace = get_type_hints(cls)
        for field in dataclasses.fields(cls):  # type: ignore[arg-type]
            ftype = namespace[field.name]
            if issubclass(ftype, ROOTSerializable):
                arg, buffer = ftype.read(buffer)
            else:
                msg = f"Cannot read field {field.name} of type {ftype}"
                raise NotImplementedError(msg)
            args.append(arg)
        return cls(*args), buffer


S = TypeVar("S", bound="StructClass")


class StructClass(ROOTSerializable):
    """ A class used to represent a structure that can be serialized and deserialized using ROOT.

    Attributes
    ----------
    _struct : struct.Struct
        A struct object that defines the format of the structure.

    Methods
    -------
    size() -> int:
        Returns the size of the structure in bytes.
    read(buffer: ReadBuffer) -> tuple[StructClass, ReadBuffer]:
        Reads the structure from the given buffer and returns an instance of the class and the remaining buffer.
    """
    _struct: struct.Struct

    @classmethod
    def size(cls) -> int:
        return cls._struct.size

    @classmethod
    def read(cls: type[S], buffer: ReadBuffer) -> tuple[S, ReadBuffer]:
        args, buffer = buffer.unpack(cls._struct)
        return cls(*args), buffer


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
