from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..structutil import (
    DataFetcher,
    ReadBuffer,
    ROOTSerializable
)

# from .RNTupleFrame import RNTupleFrame
from .RNTupleEnvelopeLink import DICTIONARY_ENVELOPE
# DICIONARY_ENVELOPE is to avoid circular imports (see RNTupleEnvelopeLink.py)

from .RNTupleFrame import (
    FrameHeader,
    RNTupleRecordFrame_SchemaExtension,
    RNTupleListFrame_ClusterGroups,
    # RNTupleRecordFrame_ClusterGroup,
    RNTupleListFrame_ClusterSummaries,
    # RNTupleRecordFrame_ClusterSummary
    RNTupleListFrame_PageLocations_Clusters,
)

# Map of envelope type to string for printing
ENVELOPE_TYPE_MAP = {
    0x01: "Header",
    0x02: "Footer",
    0x03: "Page List",
    0x04: "Unknown"
}
# abbott: should ENVELOPE_TYPE_MAP be in the get_type method? or should it be a class attribute?
#       does it matter? not functionally, but maybe there is a pythonic convention here
# leaving here for now so i can access it freely if need be

@dataclass
class RNTupleFeatureFlags(ROOTSerializable):
    """ A class representing the RNTuple Feature Flags.
    RNTuple Feature Flags appear in the Header and Footer Envelopes.
    This class reads the RNTuple Feature Flags from the buffer.
    It also checks if the flags are set for a given feature.
    It aborts reading when an unknown feature is encountered (unknown bit set).
    # abbott Q: does "gracefully abort reading" mean anything more than raising an exception?

    Attributes:
        flags (int): The RNTuple Feature Flags (signed 64-bit integer)
    """
    
    flags: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads the RNTuple Feature Flags from the given buffer.

        Args:
            buffer (ReadBuffer): The buffer to read the RNTuple Feature Flags from.

        Returns:
            RNTupleFeatureFlags: The RNTuple Feature Flags instance.
        """

        # Read the flags from the buffer
        (flags,), buffer = buffer.unpack("<q") # Signed 64-bit integer

        # There are no feature flags defined for RNTuple yet
        # So abort if any bits are set
        if flags != 0:
            raise NotImplementedError(f"Unknown feature flags encountered. int:{flags}; binary:{bin(flags)}")

        return cls(flags), buffer

@dataclass
class RNTupleEnvelope(ROOTSerializable):
    """ A class representing the RNTuple envelope structure

    Attributes:
        typeID (int): Envelope type (Header, Footer, Page List, or Unknown)
        length (int): Uncompressed size of the entire envelope (header, payload, unknown, checksum)
        payload (bytes): Envelope payload
        checksum (int): Checksum of the envelope
    """

    typeID: int
    length: int
    payload: (RNTupleHeaderEnvelope_payload | RNTupleFooterEnvelope_payload | RNTuplePageListEnvelope_payload)
    unknown: bytes | None
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads an RNTupleEnvelope from the given buffer.
        """
        print(f"\033[1;36mReading RNTuple Envelope;\033[0m {buffer.info()}")
        
        #### Get the first 64bit integer (lengthType) which contains the length and type of the envelope
        # lengthType, buffer = buffer.consume(8)
        (lengthType,), buffer = buffer.unpack("<Q")
        typeID = lengthType & 0xFFFF # Envelope type, encoded in the 16 least significant bits
        length = lengthType >> 16 # Envelope size (uncompressed), encoded in the 48 most significant bits
        if length - 8 != buffer.__len__(): # Ensure that the length of the envelope matches the buffer length
            raise ValueError(f"Length of envelope ({length} minus 8) of type {typeID} does not match buffer length ({buffer.__len__()})")

        #### Get the payload
        # relpos is guranteed to exist (consider compressed data, abspos doesn't exist)
        payload_start_pos = buffer.relpos

        if typeID == 0x01: # Header
            payload, buffer = RNTupleHeaderEnvelope_payload.read(buffer)
        elif typeID == 0x02: # Footer
            payload, buffer = RNTupleFooterEnvelope_payload.read(buffer)
        elif typeID == 0x03: # Page List
            payload, buffer = RNTuplePageListEnvelope_payload.read(buffer)
        else:
            raise ValueError(f"Unknown envelope type: {typeID}")

        #### Consume any unknown trailing information in the envelope
        unknown, buffer = buffer.consume(length - 8 - 8 - buffer.relpos + payload_start_pos)
        # Unknown Bytes = Payload Size - Payload Bytes Read
        #   Payload Size        = Envelope Size - 8 (for lengthType) - 8 (for checksum)
        #   Payload Bytes Read  = buffer.relpos - payload_start_pos

        #### Get the checksum (appended to envelope when writing to disk)
        (checksum,), buffer = buffer.unpack("<Q") # Last 8 bytes of the envelope

        print(f"\033[1;32mDone reading RNTuple Envelope! {typeID=} ({ENVELOPE_TYPE_MAP[typeID]}), {length=}\033[0m")
        return cls(typeID, length, payload, unknown, checksum), buffer

    def get_type(self) -> str:
        """ Get the envelope type as a string"""
        # If the typeID is not in the map, raise an error
        if self.typeID not in ENVELOPE_TYPE_MAP:
            raise ValueError(f"Unknown envelope type: {self.typeID}")
        return ENVELOPE_TYPE_MAP[self.typeID]

    def print_info(self) -> str:
        """ Print the envelope information """
        print(f"\033[1;35m\n-------------------------------- RNTuple {self.get_type()} Envelope Info --------------------------------\033[0m")

        for attr_name, attr_value in vars(self).items():
            # Print non-payload attributes
            if attr_name != "payload":
                print(f"{attr_name}: {attr_value}")
            # Print payload information
            else:
                print(f"\033[1;35m\t------------------------ {self.get_type()} Envelope Payload ------------------------\033[0m")
                self._print_payload(self.payload, depth=1)
                print(f"\033[1;35m\t---------------------- End {self.get_type()} Envelope Payload ----------------------\033[0m")


        print(f"\033[1;35m------------------------------ End RNTuple {self.get_type()} Envelope Info ------------------------------\n\033[0m")

    def _print_payload(self, payload: Any, depth: int) -> None:
        """ Recursively print payload attributes """
        # print(f"\033[1;33m\ttrigger payload\033[0m")
        tabs = '\t' * depth
        for attr_name, attr_value in vars(payload).items():
            if isinstance(attr_value, list): # Check if the attribute is a list
                # print(f"\033[1;33m\ttrigger list\033[0m")
                print(f"\033[1;36m{tabs}{attr_name} (type: list[{type(attr_value[0]).__name__}]):\033[0m")
                for i, item in enumerate(attr_value):
                    # print(f"{tabs}\t[{i}]:", end="\r") # Print the index of the item in the list
                    # str_color = "\033[1;34m" if hasattr(item, "__dict__") else "\033[1;36m"
                    str_color = "\033[1;34m" if ("Frame" in type(attr_value[0]).__name__) else "\033[1;36m"
                    # print(f"{tabs}\t[{i}]: {type(item).__name__}\033[0m", end="\r") # Print the index of the item in the list
                    print(f"{str_color}{tabs}\t[{i}]:\033[0m", end="\r") # Print the index of the item in the list
                    # print(f"\033[1;36m{tabs}\t[{i}]:\033[0m", end="\r") # Print the index of the item in the list
                    self._print_payload(item, depth + 2)
            elif hasattr(attr_value, "__dict__") and ("Frame" in attr_name):  # Check if the attribute has nested attributes
                # print(f"\033[1;33m\ttrigger dict\033[0m")
                print(f"\033[1;34m{tabs}{attr_name} (type: {type(attr_value).__name__}):\033[0m")
                self._print_payload(attr_value, depth + 1)
            else: # Print the attribute
                # print(f"\033[1;33m\ttrigger else\033[0m")
                print(f"{tabs}{attr_name}: {attr_value}")

DICTIONARY_ENVELOPE[b"RNTupleEnvelope"] = RNTupleEnvelope

@dataclass
class RNTupleHeaderEnvelope_payload(ROOTSerializable):
    """ A class representing the RNTuple Header Envelope payload structure
    """

# abbott TODO: look at class hierarchy stuff here like in envelope link / locator
@dataclass
class RNTupleFooterEnvelope_payload(ROOTSerializable):
    """ A class representing the RNTuple Footer Envelope payload structure.

    Attributes:
        featureFlags (RNTupleFeatureFlags): The RNTuple Feature Flags (verify this file can be read)
        headerChecksum (int): The checksum of the Header Envelope
        schemaExtensionRecordFrame (RNTupleFrame): The Schema Extension Record Frame
        clusterGroupListFrames (RNTupleListFrame_ClusterGroups): The List Frame of Cluster Group Record Frames
    """

    featureFlags: RNTupleFeatureFlags
    headerChecksum: int
    schemaExtensionRecordFrame: RNTupleRecordFrame_SchemaExtension
    clusterGroupListFrame: RNTupleListFrame_ClusterGroups

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads the RNTuple Footer Envelope Payload from the given buffer.
        """
        print(f"\033[1;36m\tReading RNTuple Footer Envelope Payload;\033[0m {buffer.info()}")
        
        #### Read the feature flags
        featureFlags, buffer = RNTupleFeatureFlags.read(buffer)

        #### Read the header envelope checksum (verify later in RNTuple class read method)
        (headerChecksum,), buffer = buffer.unpack("<Q")

        #### Read the Schema Extension Record Frame

        schemaExtensionRecordFrame, buffer = RNTupleRecordFrame_SchemaExtension.read(buffer)
        # Verify that we read a record frame
        if schemaExtensionRecordFrame.fheader.fType != 0:
            raise ValueError(f"Expected a (scheme extension) record frame, but got a frame of type {schemaExtensionRecordFrame.fheader.fType=}")

        #### Read the List Frame of Cluster Group Record Frames
        
        clusterGroupListFrame, buffer = RNTupleListFrame_ClusterGroups.read(buffer)
        # Verify that we read a list frame
        if clusterGroupListFrame.fheader.fType != 1:
            raise ValueError(f"Expected a (cluster group) list frame, but got a frame of type {clusterGroupListFrame.fheader.fType=}")

        print(f"\033[1;32m\n\tDone reading RNTuple Footer Envelope Payload!\033[0m {buffer.info()}")
        return cls(featureFlags, headerChecksum, schemaExtensionRecordFrame, clusterGroupListFrame), buffer


    # abbott TODO: check out rich.print for pretty printing
    def get_pagelist(self, fetch_data: DataFetcher) -> list[RNTupleEnvelope]:
        """ Get the RNTuple Page List Envelopes from the Footer Envelope.

        Page List Envelope Links are stored in the Cluster Group Record Frames in the Footer Envelope Payload.
        """

        print(f"\033[1;36mReading RNTuple Page Lists\033[0m")

        #### Get the Page List Envelopes
        pagelist_envelopes = [] # List of RNTuple Page List Envelopes

        ### Iterate through the Cluster Group Record Frames
        for i, clusterGroupListFrame in enumerate(self.clusterGroupListFrame.clusterGroupRecordFrames):
            ## The cluster group record frame contains other info will be useful later.
            #       i.e. Minimum Entry Number, Entry Span, and Number of Clusters.
            # For now, we only need the Page List Envelope Link.

            # Read the page list envelope
            print(f"\033[1;36m---- Reading RNTuple Page List Envelope {i} ----\033[0m")
            pagelist_envelope = clusterGroupListFrame.pagelistLink.read_envelope(fetch_data)
            pagelist_envelopes.append(pagelist_envelope)
            print(f"\033[1;32m---- Done reading RNTuple Page List Envelope {i}! ----\033[0m\n")

        print(f"\033[1;32mDone reading RNTuple Page Lists!\033[0m\n")
        return pagelist_envelopes


@dataclass
class RNTuplePageInfo(ROOTSerializable):
    """ A class representing the RNTuple Page Info structure.
    """

@dataclass
class RNTuplePageListEnvelope_payload(ROOTSerializable):
    """ A class representing the RNTuple Page List Envelope payload structure.

    Attributes:
    headerChecksum (int): The checksum of the Header Envelope
    clusterSummaryListFrame (list[RNTupleFrame]): The list of Cluster Summary Record Frames
    page_locations (RNTupleListFrame_PageLocations_Clusters): The triple nested list of page locations
    """

    headerChecksum: int
    clusterSummaryListFrame: RNTupleListFrame_ClusterSummaries
    pageLocationsNestedListFrame: RNTupleListFrame_PageLocations_Clusters

    @classmethod
    def read(cls, buffer: ReadBuffer):
        """ Reads the RNTuple Page List Envelope Payload from the given buffer.
        """
        print(f"\033[1;36m\tReading RNTuple Page List Envelope Payload;\033[0m {buffer.info()}")
        
        #### Read the header envelope checksum (verify later in RNTuple class read method)
        (headerChecksum,), buffer = buffer.unpack("<Q")

        #### Read the List Frame of Cluster Summary Record Frames
        clusterSummaryListFrame, buffer = RNTupleListFrame_ClusterSummaries.read(buffer)
        # Verify that we read a list frame
        if clusterSummaryListFrame.fheader.fType != 1:
            raise ValueError(f"Expected a (cluster summary) list frame, but got a frame of type {clusterSummaryListFrame.fheader.fType=}")

        # print(f"{clusterSummaryListFrame=}")
        # abbbott TODO: check flag for sharded cluster

        #### Read the Nested List Frame of Page Locations
        pageLocationsNestedListFrame, buffer = RNTupleListFrame_PageLocations_Clusters.read(buffer)
        # Verify that we read a list frame
        if pageLocationsNestedListFrame.fheader.fType != 1:
            raise ValueError(f"Expected a (page locations) list frame, but got a frame of type {pageLocationsNestedListFrame.fheader.fType=}")

        print(f"\033[1;32m\n\tDone reading RNTuple Page List Envelope Payload!\033[0m {buffer.info()}")
        return cls(headerChecksum, clusterSummaryListFrame, pageLocationsNestedListFrame), buffer

    def get_pages(self, fetch_data: DataFetcher) -> list[list[list[RNTuplePageInfo]]]:
        """ Get the RNTuple Pages from the Page Locations Nested List Frame.
        """
        print(f"\033[1;36mReading RNTuple Pages\033[0m")
        
        #### Get the Pages
        pages = self.pageLocationsNestedListFrame.read_list(fetch_data)

        print(f"\033[1;32mDone reading RNTuple Pages!\033[0m\n")
        return pages
