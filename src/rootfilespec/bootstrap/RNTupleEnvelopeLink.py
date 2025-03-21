from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable,
    # StructClass,
    # sfield,
    # structify,
)

# from .RNTupleEnvelope import RNTupleEnvelope
# Use map to avoid circular imports
DICTIONARY_ENVELOPE: dict[bytes, type[ROOTSerializable]] = {}

''' # Envelope Link without Locator classes, keeping in case locators break everything
@dataclass
class RNTupleEnvelopeLink(ROOTSerializable):
    """ A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).
    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link is consists of a 64 bit unsigned integer that specifies the 
        uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the RNTuple Anchor.
        In this case, the create_envelope_links_from_anchor() method should be used.
    """

    # While envelope links & locators have different formats, these attributes will always mean the same thing
    # This will break when non standard locators that are not defined by size, type, length, and offset are introduced.
    offset: int # Byte offset to the envelope
    size: int # Compressed size of the envelope
    length: int # Uncompressed size of the envelope
    


    # locator: RNTupleLocator # Locator 

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a RNTuple envelope link from the given buffer.
        """
        # print(f"\033[1;36m\tReading RNTupleEnvelopeLink;\033[0m {buffer.info()}")

        # All envelope links start with the envelope length
        (length,), buffer = buffer.unpack("<Q")

        # Read the locator
        # locator, buffer = RNTupleLocator.read(buffer)

        # Load the first 32 bit integer in the buffer to determine the locator type
        (sizeType,), buffer = buffer.unpack("<i")

        if sizeType >= 0: # Standard Locator if sizeType is positive
            size = abs(sizeType) # Compressed size of the envelope
            type = 0 # Standard locator type
            (offset,), buffer = buffer.unpack("<Q") # Byte offset to the envelope
        else: # Non-standard Locator if sizeType is negative
            size_locator = sizeType & 0xFFFF # Size of locator itself (16 least significant bits)
            reserved = (sizeType >> 16) & 0xFF # Reserved for future use (8 next least significant bits)
            type = (sizeType >> 24) & 0xFF # Type of non-standard locator (8 most significant bits)

            if type == 0x01: # Large Locator
                # Similar to standard locator, but with 64 bit size 
                size, offset, buffer = buffer.unpack("<QQ")
            else:
                raise ValueError(f"Unknown non-standard locator type: {type}")

        # print(f"\033[1;32m\tDone reading RNTupleEnvelopeLink!\033[0m")
        return cls(offset, size, length), buffer

    def read_envelope(
        self,
        fetch_data: DataFetcher,
    # ) -> RNTupleEnvelope:
    ) -> ROOTSerializable:
        
        """ Reads an RNTupleEnvelope from the given buffer.
        """        
        # Load the (possibly compressed) envelope into the buffer
        # This should load exactly the envelope bytes (no more, no less)
        buffer = fetch_data(self.offset, self.size)
        
        print(f"\033[1;36m\tAccessing RNTuple Envelope Link;\033[0m {buffer.info()}")

        compressed = None
        if self.size != self.length:
            raise NotImplementedError("Compressed envelopes are not yet supported")
            # TODO: Implement compressed envelopes

        # Now the envelope is uncompressed
        
        # envelope, buffer = RNTupleEnvelope.read(buffer)
        envelope, buffer = DICTIONARY_ENVELOPE[b"RNTupleEnvelope"].read(buffer)

        # check that buffer is empty
        if buffer:
            msg = f"RNTupleEnvelopeLink.read_envelope: buffer not empty after reading envelope of type {envelope.typeID}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{envelope=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        
        print(f"\033[1;32m\tDone reading RNTuple Envelope Link!\033[0m {buffer.info()}")
        return envelope
'''


@dataclass
class RNTupleEnvelopeLink(ROOTSerializable):
    """ A class representing the RNTuple Envelope Link (somewhat analogous to a TKey).
    An Envelope Link references an Envelope in an RNTuple.
    An Envelope Link is consists of a 64 bit unsigned integer that specifies the 
        uncompressed size (i.e. length) of the envelope, followed by a Locator.

    Envelope Links of this form (currently seem to be) only used to locate Page List Envelopes.
    The Header Envelope and Footer Envelope are located using the information in the RNTuple Anchor.
    The Header and Footer Envelope Links are created in the RNTuple Anchor class by casting directly to this class.

    Attributes:
        length (int): Uncompressed size of the envelope
        locator (RNTupleLocator): Locator for the envelope
    """

    length: int # Uncompressed size of the envelope
    locator: RNTupleLocator # Locator base class
    
    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a RNTuple envelope link from the given buffer.
        """
        # print(f"\033[1;36m\tReading RNTupleEnvelopeLink;\033[0m {buffer.info()}")

        # All envelope links start with the envelope length
        (length,), buffer = buffer.unpack("<Q")

        # Read the locator
        locator, buffer = RNTupleLocator.read(buffer)

        # print(f"\033[1;32m\tDone reading RNTupleEnvelopeLink!\033[0m")
        return cls(length, locator), buffer

    def get_buffer(self, fetch_data: DataFetcher):
        """ Returns the buffer for the byte range specified by the locator.
        """
        return self.locator.get_buffer(fetch_data)

    def is_compressed(self) -> bool:
        """ Returns whether the envelope is compressed.
        e.g. if the (compressed) size of the envelope is different from the (uncompressed) length of the envelope.
        """

        # abbott Q: this is not polymorphic...

        # Check each type of locator for compression
        if self.locator.isStandard: # Standard locator
            return self.locator.locatorSubclass.size != self.length
        elif self.locator.locatorSubclass.locatorType == 0x01: # Large locator
            return self.locator.locatorSubclass.payload.size != self.length
        else:
            raise NotImplementedError(f"Non-standard locators of this type are not yet supported: {self.locator.locatorSubclass.locatorType=}")

    def read_envelope(self, fetch_data: DataFetcher) -> ROOTSerializable:
        """ Reads an RNTupleEnvelope from the given buffer.
        """        
        # Load the (possibly compressed) envelope into the buffer
        # This should load exactly the envelope bytes (no more, no less)
        # buffer = fetch_data(self.offset, self.size)
        
        buffer = self.locator.get_buffer(fetch_data)
        
        print(f"\033[1;36mAccessing RNTuple Envelope Link;\033[0m {buffer.info()}")

        # If compressed, decompress the envelope
        # compressed = None
        compressed = self.is_compressed()
        # if self.size != self.length:
        if self.is_compressed():
            raise NotImplementedError("Compressed envelopes are not yet supported")
            # TODO: Implement compressed envelopes

        # Now the envelope is uncompressed
        
        # envelope, buffer = RNTupleEnvelope.read(buffer)
        envelope, buffer = DICTIONARY_ENVELOPE[b"RNTupleEnvelope"].read(buffer)

        # check that buffer is empty
        if buffer:
            msg = f"RNTupleEnvelopeLink.read_envelope: buffer not empty after reading envelope of type {envelope.typeID}."
            msg += f"\n{self=}"
            msg += f"\n{compressed=}"
            msg += f"\n{envelope=}"
            msg += f"\nBuffer: {buffer}"
            raise ValueError(msg)
        
        print(f"\033[1;32mDone reading RNTuple Envelope Link!\033[0m {buffer.info()}")
        return envelope
    


@dataclass
class RNTupleLocator(ROOTSerializable):
    """ A base class representing a Locator for RNTuples.
    A locator is a generalized way to specify a certain byte range on the storage medium. 
        For disk-based storage, the locator is just byte offset and byte size. 
        For other storage systems, the locator contains enough information to retrieve the referenced block, 
            e.g. in object stores, the locator can specify a certain object ID.

    All locators begin with a signed 32 bit integer.
    If the integer is positive, the locator is a standard locator.
    For standard locators, the size of the byte range to locate is the absolute value of the integer.
    If the integer is negative, the locator is a non-standard locator.
    Size and type mean different things for standard and non-standard locators. 

    This base class checks the type of the locator and reads the appropriate subclass.
    It contains a `get_buffer()` method that should be implemented by subclasses to return the buffer for the envelope.
    This provides forward compatibility for different locator types, as the envelope can be read using the same method.

    All Envelope Links will have a Locator, but a Locator doesn't require an Envelope Link.
    (See the Page Location in the Page List Envelopes for an example of a Locator without an Envelope Link.)
    
    Attributes:
        isStandard (bool): Whether the locator is a standard locator.
        locatorSubclass (RNTupleStandardLocator | RNTupleNonStandardLocator): The subclass of the locator.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleLocator: Reads a RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the locator.
    """

    # abbott TODO: this
    # isStandard should not be a "data member"
    # data members = things that are in the file
    # isStandard should be a property that checks what the subclass
    # properties appear as attributes but don't appear in the same way in the class
    isStandard: bool
    locatorSubclass: RNTupleStandardLocator | RNTupleNonStandardLocator

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a RNTuple locator from the given buffer.
        """
        # print(f"\033[1;36mReading RNTupleLocator;\033[0m {buffer.info()}")
        # Peek (don't update buffer) at the first 32 bit integer in the buffer to determine the locator type
        (sizeType,), _ = buffer.unpack("<i")

        isStandard = sizeType >= 0 # Standard Locator if sizeType is positive

        if isStandard:
            locatorSubclass, buffer = RNTupleStandardLocator.read(buffer)
        else:
            locatorSubclass, buffer = RNTupleNonStandardLocator.read(buffer)
        # print(f"{isStandard=}, {locatorSubclass=}")
        # print(f"\033[1;32mDone reading RNTupleLocator!\033[0m {buffer.info()}")
        return cls(isStandard, locatorSubclass), buffer
    
    def get_buffer(self, fetch_data: DataFetcher):
        """ Returns the buffer for the byte range specified by the locator.
        """
        return self.locatorSubclass.get_buffer(fetch_data)
    
@dataclass
# abbott TODO: fix polymorphism here
# to get to polymorphism, need to provide the class directly
# e.g. class RNTupleStandardLocator(RNTupleLocator)
class RNTupleStandardLocator(ROOTSerializable):
    """ A class representing a Standard RNTuple Locator.
    A locator is a generalized way to specify a certain byte range on the storage medium.
    A standard locator is a locator that specifies a byte size and byte offset. (simple on-disk or in-file locator).

    Attributes:
        size (int): The size of the byte range.
        offset (int): The byte offset to the byte range.
    
    Methods:
        read(buffer: ReadBuffer) -> RNTupleStandardLocator: Reads a standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the standard locator.
    """

    size: int
    offset: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a standard RNTuple locator from the given buffer.
        """

        # Size is the absolute value of a signed 32 bit integer
        (size,), buffer = buffer.unpack("<i")
        size = abs(size)

        # Offset is a 64 bit unsigned integer
        (offset,), buffer = buffer.unpack("<Q")

        return cls(size, offset), buffer
    
    def get_buffer(self, fetch_data: DataFetcher):
        """ Returns the buffer for the byte range specified by the standard locator.
        """
        return fetch_data(self.offset, self.size)

@dataclass    
class RNTupleNonStandardLocator(ROOTSerializable):
    """ A base class representing a Non-Standard RNTuple Locator.
    A non-standard locator can specify additional information beyond just the size and offset.
    This class is a base class for different types of non-standard locators.
    All non-standard locators have the same header format, but unique payloads.

    Attributes:
        locatorSize (int): The (uncompressed) size of the locator itself.
        reserved (int): Reserved by ROOT developers for future use.
        locatorType (int): The type of the non-standard locator.
        payload (RNTupleLargeLocator_payload): The payload of the non-standard locator.

    Notes:
    Currently available non-standard locator types:

        Type	Meaning	        Payload format
        0x01	Large locator	64bit size followed by 64bit offset

    The range 0x02 - 0x7f is reserved for future use.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleNonStandardLocator: Reads a non-standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the non-standard locator.
    """

    locatorSize: int
    reserved: int
    locatorType: int
    payload: RNTupleLargeLocator_payload

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a non-standard RNTuple locator from the given buffer.
        """

        # The first 32 bit signed integer contains the locator size, reserved, and locator type
        (locatorSizeReservedType,), buffer = buffer.unpack("<i")

        # The locator size is the 16 least significant bits
        locatorSize = locatorSizeReservedType & 0xFFFF # Size of locator itself
        
        # The reserved field is the next 8 least significant bits
        reserved = (locatorSizeReservedType >> 16) & 0xFF # Reserved by ROOT developers for future use

        # The locator type is the 8 most significant bits (the final 8 bits) 
        locatorType = (locatorSizeReservedType >> 24) & 0xFF # Type of non-standard locator

        # Read the payload based on the locator type
        if locatorType == 0x01:
            payload, buffer = RNTupleLargeLocator_payload.read(buffer)
        else:
            raise ValueError(f"Unknown non-standard locator type: {locatorType=}")

        return cls(locatorSize, reserved, locatorType, payload), buffer
    
    def get_buffer(self, fetch_data: DataFetcher):
        """ Returns the buffer for the byte range specified by the non-standard locator.
        """
        return self.payload.get_buffer(fetch_data)

@dataclass
class RNTupleLargeLocator_payload(ROOTSerializable):
    """ A class representing the payload of the "Large" type of Non-Standard RNTuple Locator .
    A Large Locator is like the standard on-disk locator but with a 64bit size instead of 32bit.
    The type for the Large Locator is 0x01 (specified in the RNTupleNonStandardLocator base class).

    Attributes:
        size (int): The (compressed) size of the byte range.
        offset (int): The byte offset to the byte range.

    Methods:
        read(buffer: ReadBuffer) -> RNTupleLargeLocator_payload: Reads a payload for a "Large" type of Non-Standard RNTuple locator from the given buffer.
        get_buffer(fetch_data: DataFetcher) -> ReadBuffer: Returns the buffer for the byte range specified by the "Large Locator" payload.
    """

    size: int
    offset: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads a payload for a "Large" type of Non-Standard RNTuple locator from the given buffer.
        """

        # Size is a 64 bit unsigned integer
        (size,), buffer = buffer.unpack("<Q")

        # Offset is a 64 bit unsigned integer
        (offset,), buffer = buffer.unpack("<Q")

        return cls(size, offset), buffer
    
    def get_buffer(self, fetch_data: DataFetcher):
        """ Returns the buffer for the byte range specified by the "Large Locator" payload.
        """
        return fetch_data(self.offset, self.size)
