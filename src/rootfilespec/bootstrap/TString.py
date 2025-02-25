from __future__ import annotations

from dataclasses import dataclass

from ..structutil import ReadBuffer, ROOTSerializable


@dataclass
class TString(ROOTSerializable):
    """ A class representing a TString.

    Attributes:
        fString (bytes): The byte representation of the string.

    Methods:
        read(cls, buffer: ReadBuffer) -> Tuple[TString, ReadBuffer]:
            Reads a TString from the given buffer and returns the TString instance along with the remaining buffer.
    """

    fString: bytes

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a TString from the given buffer.
        TStrings are always prefixed with a byte indicating the length of the string.

        Args:
            buffer (ReadBuffer): The buffer to read the TString from. 

        Returns:
            Tuple[TString, ReadBuffer]: A tuple containing the TString instance and the remaining buffer.
        """
        (length,), buffer = buffer.unpack(">B")
        if length == 255:
            (length,), buffer = buffer.unpack(">i")
        data, buffer = buffer.consume(length)
        return cls(data), buffer
