from __future__ import annotations

from rootfilespec.bootstrap.TKey import DICTIONARY
from rootfilespec.bootstrap.TObject import StreamHeader
from rootfilespec.structutil import ReadBuffer, ROOTSerializable


def read_streamed_item(buffer: ReadBuffer) -> tuple[ROOTSerializable, ReadBuffer]:
    # Read ahead the stream header to determine the type of the object
    itemheader, _ = StreamHeader.read(buffer)
    if itemheader.fClassName not in DICTIONARY:
        msg = f"Unknown class name: {itemheader.fClassName}"
        msg += f"\nStreamHeader: {itemheader}"
        raise ValueError(msg)
    # Now actually read the object
    item, buffer = DICTIONARY[itemheader.fClassName].read(buffer)
    return item, buffer
