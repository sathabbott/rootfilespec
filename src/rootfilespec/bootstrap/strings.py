from typing import Annotated

from rootfilespec.structutil import ROOTString

TString = Annotated[bytes, ROOTString("TString")]
"""A ``TString`` data member, read as plain ``bytes``

A counted string: one length byte, or 255 then a 4-byte length, then the bytes
(root-io-spec Conventions §5.1). ``TString`` has no Python class: a member is
``bytes``, and the encoding lives in the annotation. Read a bare one by hand
with ``read_string(buffer, "TString")``.
"""

TStringLong = Annotated[bytes, ROOTString("charstar")]
"""A ``TStringLong``, read as plain ``bytes``

``TStringLong`` derives from ``TString`` but writes an i32 length, then the bytes,
with no 255 escape and no frame, wherever it appears: the ``char*`` encoding
(root-io-spec Conventions §5.1.1).
"""

string = TString
"""A ``std::string`` looked up by name, as an object of its own (a key's class)

It is the bare counted string, as a ``TString`` is. A ``std::string`` data member
has a byte count and version word first: ``Annotated[bytes, ROOTString("std::string")]``.
"""
