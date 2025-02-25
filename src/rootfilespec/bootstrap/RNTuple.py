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

from .streamedobject import StreamHeader
from .TKey import DICTIONARY
from .RNTupleAnchor import RNTupleAnchor

@dataclass
class RNTuple(ROOTSerializable):
    """ RNTuple object
    Binary Specification: https://github.com/root-project/root/blob/v6-34-00-patches/tree/ntuple/v7/doc/BinaryFormatSpecification.md
    Attributes:
        anchor (RNTupleAnchor): RNTuple Anchor information
    """
    
    anchor: RNTupleAnchor

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\tReading RNTuple; {buffer.info()}\033[0m")
        anchor, buffer = RNTupleAnchor.read(buffer)
        print(anchor)

        print(f"\033[1;32m\tDone reading RNTuple\n\033[0m")
        return cls(anchor), buffer

DICTIONARY[b"ROOT::RNTuple"] = RNTuple
