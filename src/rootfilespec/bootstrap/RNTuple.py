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

# from .streamedobject import StreamHeader
from .TKey import DICTIONARY
from .RNTupleAnchor import RNTupleAnchor

@dataclass
class RNTupleEnvelope(ROOTSerializable):
    """ A class representing the RNTuple envelope structure

    Attributes:
        typeID (int): Envelope type (Header, Footer, Page List, or Unknown)
        length (int): Uncompressed size of the entire envelope (header, payload, padding, checksum)
        payload (bytes): Envelope payload
        checksum (int): Checksum of the envelope
    """

    typeID: int
    length: int
    payload: bytes
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\tReading RNTupleEnvelope; {buffer.info()}\033[0m")
        # Envelope type ID: Encoded in the 16 least significant bits of the first 64bit integer
        # Envelope length: Encoded in the 48 most significant bits of the first 64bit integer
        # Get the first 64bit integer
        envelope_header, buffer = buffer.consume(8)
        typeID = envelope_header & 0xFFFF # 16 least significant bits
        length = envelope_header >> 16 # 48 most significant bits
        if length - 8 != buffer.__len__(): # Ensure that the length of the envelope matches the buffer length
            raise ValueError(f"Length of envelope ({length} minus 8) of type {typeID} does not match buffer length ({buffer.__len__()})")
        
        payload, buffer = buffer.consume(length - 8 - 8) # Subtract 8 bytes for the header and 8 bytes for the checksum
        checksum, buffer = buffer.unpack(">Q") # Get the checksum (last 8 bytes)

        print(f"\033[1;32m\tDone reading RNTupleEnvelope\n\033[0m")
        return cls(typeID, length, payload, checksum), buffer
    
    # abbott pick up here
    # abbott: add a read_payload method similar to TKey.read_object?
    #       Maybe read_envelope bc envelopes and pages can be compressed
    #       and since compression is handled at read_object() that makes sense
    #       test file is uncompressed so allows for me to do this while handling compression later 
    # something like:
    """ put this in the RNTuple class i think
    def get_envelope(self, fetch_data: DataFetcher) -> RNTupleEnvelope:
        buffer = fetch_data(
            self.fSeekEnvelope, self.header.fNbytesEnvelope
        )
        <compression placeholder code>
        # Then uncompressed code:
        envelope, buffer = RNTupleEnvelope.read(buffer)
        # Now we have the envelope type and payload
        typename = envelope.getTypeName() # Add a function to  to get the typename from the typeID
        obj, buffer = DICTIONARY[typename].read(buffer) # This will read any payload (Header, Footer, Page List, etc.)
        return envelope, obj?

        To know if this structure works, I will need to look through the code I have for the 
        footer and page list envelopes and see what info is needed. i suspect we need to cross 
        reference a lot of info (at the very least the header checksums with the footer and page list)
        so it makes sense to keep this in the RNTuple class so I can access everything in one place
        
        Therefore I will prolly need to get partway through this, then go and draft the code for the footer
        and page list (footer first, page list is more complex) and then come back to this
        and then pray that the header doesn't screw it up

    """




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
    
    # def get_RNTupleFooter


DICTIONARY[b"ROOT::RNTuple"] = RNTuple
