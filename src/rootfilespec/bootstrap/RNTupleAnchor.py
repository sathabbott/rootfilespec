from __future__ import annotations

from dataclasses import dataclass

from ..structutil import (
    ReadBuffer,
    ROOTSerializable,
    StructClass,
    sfield,
    structify,
)
from .RNTupleEnvelopeLink import (
    RNTupleEnvelopeLink,
    RNTupleLocator,
    RNTupleStandardLocator,
)
from .streamedobject import StreamHeader
from .TKey import DICTIONARY

'''
@dataclass
class RNTuple(ROOTSerializable):
    """ The main RNTuple object.
    Binary Specification: https://github.com/root-project/root/blob/v6-34-00-patches/tree/ntuple/v7/doc/BinaryFormatSpecification.md
    Attributes:
        anchor (RNTupleAnchor): Contains the RNTuple metadata
        header (RNTupleEnvelope): Contains RNTuple schema (field and column types)
        footer (RNTupleEnvelope): Contains description of RNTuple clusters & location of page list envelope
        page_list (RNTupleEnvelope): Contains the location of RNTuple data pages
    """

    anchor: RNTupleAnchor
    header: RNTupleEnvelope
    footer: RNTupleEnvelope
    page_list: RNTupleEnvelope

    @classmethod
    def read(cls, buffer: ReadBuffer, fetch_data: DataFetcher):
        print(f"\033[1;36m\nReading RNTuple;\n\033[0m {buffer.info()}")

        # Read the RNTuple Anchor
        anchor, buffer = RNTupleAnchor.read(buffer)
        print(f"{anchor}\n")

        # Read the RNTuple Header Envelope
        # header, buffer = RNTupleEnvelope.read(buffer, header_link)
        # TODO: Implement the RNTupleHeaderEnvelope_payload class




        # Read the RNTuple Footer Envelope
        footer = anchor.get_footer(fetch_data)
        # footer, buffer = footer_link.read_envelope(buffer)

        # Verify the checksums

        # Read the RNTuple Page List Envelope
        # page_list, buffer = RNTupleEnvelope.read(buffer, page_list_link)
        # TODO: Implement the RNTuplePageListEnvelope_payload class


        print(f"\033[1;32m\tDone reading RNTuple\n\033[0m")
        return cls(anchor, footer), buffer
'''


@structify(big_endian=True)
@dataclass(order=True)
class RNTupleVersion(StructClass):
    """A class representing the RNTuple Version

    Attributes:
        fVersionEpoch (int): Version Epoch
        fVersionMajor (int): Version Major
        fVersionMinor (int): Version Minor
        fVersionPatch (int): Version Patch
    """

    # fVersionEpoch: int = sfield("H")
    # fVersionMajor: int = sfield("H")
    # fVersionMinor: int = sfield("H")
    # fVersionPatch: int = sfield("H")

    epoch: int = sfield("H")
    major: int = sfield("H")
    minor: int = sfield("H")
    patch: int = sfield("H")


@dataclass
class RNTupleAnchor(ROOTSerializable):
    """RNTuple Anchor object

    Attributes:
        sheader (StreamHeader): Stream Header information
        fVersion (RNTupleVersion): RNTuple Version
        headerLink (RNTupleEnvelopeLink): Envelope Link to the RNTuple Header Envelope
        footerLink (RNTupleEnvelopeLink): Envelope Link to the RNTuple Footer Envelope
        fMaxKeySize (int): Maximum size of an RBlob
        unknown (bytes): Unknown bytes after the Anchor, before the checksum
        checksum (int): Checksum of the Anchor
    """

    sheader: StreamHeader
    fVersion: RNTupleVersion
    headerLink: RNTupleEnvelopeLink
    # fSeekHeader (int): Offset to the header envelope
    # fNBytesHeader (int): Compressed size of the header envelope
    # fLenHeader (int): Uncompressed size of the header envelope
    footerLink: RNTupleEnvelopeLink
    # fSeekFooter (int): Offset to the footer envelope
    # fNBytesFooter (int): Compressed size of the footer envelope
    # fLenFooter (int): Uncompressed size of the footer envelope
    fMaxKeySize: int
    unknown: bytes | None
    checksum: int

    @classmethod
    def read(cls, buffer: ReadBuffer):
        print(f"\033[1;36m\nReading RNTupleAnchor;\033[0m {buffer.info()}")

        #### Read the StreamHeader (every named class has a StreamHeader)
        sheader, buffer = StreamHeader.read(buffer)

        #### Read the RNTuple Version
        fVersion, buffer = RNTupleVersion.read(buffer)

        #### Read the RNTuple Header Envelope Link
        (fSeekHeader, fNBytesHeader, fLenHeader), buffer = buffer.unpack(">QQQ")
        """ # headerLink code before adding locator class
        headerLink = RNTupleEnvelopeLink(fSeekHeader, fNBytesHeader, fLenHeader)

        # Verify the header link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fSeekHeader != headerLink.offset:
            raise ValueError(f"RNTupleAnchor.read: {fSeekHeader=} != {headerLink.offset=}")
        if fNBytesHeader != headerLink.size:
            raise ValueError(f"RNTupleAnchor.read: {fNBytesHeader=} != {headerLink.size=}")
        if fLenHeader != headerLink.length:
            raise ValueError(f"RNTupleAnchor.read: {fLenHeader=} != {headerLink.length=}")
        """

        headerLink = RNTupleEnvelopeLink(
            fLenHeader,
            RNTupleLocator(True, RNTupleStandardLocator(fNBytesHeader, fSeekHeader)),
        )

        # Verify the header link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fLenHeader != headerLink.length:
            raise ValueError(
                f"RNTupleAnchor.read: {fLenHeader=} != {headerLink.length=}"
            )
        if fNBytesHeader != headerLink.locator.locatorSubclass.size:
            raise ValueError(
                f"RNTupleAnchor.read: {fNBytesHeader=} != {headerLink.locator.locatorSubclass.size=}"
            )
        if fSeekHeader != headerLink.locator.locatorSubclass.offset:
            raise ValueError(
                f"RNTupleAnchor.read: {fSeekHeader=} != {headerLink.locator.locatorSubclass.offset=}"
            )

        #### Read the RNTuple Footer Envelope Link
        (fSeekFooter, fNBytesFooter, fLenFooter), buffer = buffer.unpack(">QQQ")
        """ # footerLink code before adding locator class
        footerLink = RNTupleEnvelopeLink(fSeekFooter, fNBytesFooter, fLenFooter)

        # Verify the footer link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fSeekFooter != footerLink.offset:
            raise ValueError(f"RNTupleAnchor.read: {fSeekFooter=} != {footerLink.offset=}")
        if fNBytesFooter != footerLink.size:
            raise ValueError(f"RNTupleAnchor.read: {fNBytesFooter=} != {footerLink.size=}")
        if fLenFooter != footerLink.length:
            raise ValueError(f"RNTupleAnchor.read: {fLenFooter=} != {footerLink.length=}")
        """

        footerLink = RNTupleEnvelopeLink(
            fLenFooter,
            RNTupleLocator(True, RNTupleStandardLocator(fNBytesFooter, fSeekFooter)),
        )

        # Verify the footer link (In case RNTupleEnvelopeLink class gets updated with more complicated formats)
        if fLenFooter != footerLink.length:
            raise ValueError(
                f"RNTupleAnchor.read: {fLenFooter=} != {footerLink.length=}"
            )
        if fNBytesFooter != footerLink.locator.locatorSubclass.size:
            raise ValueError(
                f"RNTupleAnchor.read: {fNBytesFooter=} != {footerLink.locator.locatorSubclass.size=}"
            )
        if fSeekFooter != footerLink.locator.locatorSubclass.offset:
            raise ValueError(
                f"RNTupleAnchor.read: {fSeekFooter=} != {footerLink.locator.locatorSubclass.offset=}"
            )

        #### Read the Maximum Key Size
        (fMaxKeySize,), buffer = buffer.unpack(">Q")

        """ TODO: Use fMaxKeySize to check if the RBlobs are too big
                    Max Key Size represents the maximum size of an RBlob (associated to one TFile key).
                    Payloads bigger than that size will be written as multiple RBlobs/TKeys, and the
                    offsets of all but the first RBlob will be written at the end of the first one.
                    This allows bypassing the inherent TKey size limit of 1 GiB.
        """

        # abbott Q: Ask nick if this is ok for anchor
        #### Consume any unknown trailing information in the anchor
        unknown, buffer = buffer.consume(buffer.__len__() - 8)
        # The buffer contains exactly and only the anchor bytes (including the checksum)

        #### Get the checksum (appended to anchor when writing to disk)
        (checksum,), buffer = buffer.unpack(">Q")  # Last 8 bytes of the anchor

        print(f"\033[1;32mDone reading RNTupleAnchor\033[0m {buffer.info()}\n")
        return cls(
            sheader, fVersion, headerLink, footerLink, fMaxKeySize, unknown, checksum
        ), buffer

    def print_info(self) -> str:
        print(
            "\033[1;35m\n-------------------------------- RNTuple Anchor Info --------------------------------\033[0m"
        )
        for var in vars(self).items():
            print(f"{var}")
        print(
            "\033[1;35m------------------------------ End RNTuple Anchor Info ------------------------------\n\033[0m"
        )


DICTIONARY[b"ROOT::RNTuple"] = RNTupleAnchor
