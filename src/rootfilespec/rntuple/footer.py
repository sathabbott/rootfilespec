from typing import Annotated

from rootfilespec.buffer import DataFetcher
from rootfilespec.rntuple.envelope import (
    ENVELOPE_TYPE_MAP,
    REnvelope,
    REnvelopeLink,
    RFeatureFlags,
)
from rootfilespec.rntuple.pagelist import PageListEnvelope
from rootfilespec.rntuple.RFrame import ListFrame, RecordFrame
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ExtraTypeInformation,
    FieldDescription,
)
from rootfilespec.serializable import serializable
from rootfilespec.structutil import Fmt


@serializable
class ClusterGroup(RecordFrame):
    """A class representing an RNTuple Cluster Group Record Frame.
    This Record Frame is found in a List Frame in the Footer Envelope of an RNTuple.
    It references the Page List Envelopes for groups of clusters in the RNTuple.
    """

    fMinEntryNumber: Annotated[int, Fmt("<Q")]
    """The minimum of the first entry number across all of the clusters in the group."""
    fEntrySpan: Annotated[int, Fmt("<Q")]
    """The number of entries that are covered by this cluster group."""
    fNClusters: Annotated[int, Fmt("<I")]
    """The number of clusters in the group."""
    pagelistLink: REnvelopeLink
    """Envelope Link to the Page List Envelope for the cluster group."""


@serializable
class SchemaExtension(RecordFrame):
    """A class representing an RNTuple Schema Extension Record Frame.
    This Record Frame is found in the Footer Envelope of an RNTuple.

    The schema extension record frame contains an additional schema description that is incremental with respect to
            the schema contained in the header (see Section Header Envelope). Specifically, it is a record frame with
            the following four fields (identical to the last four fields in Header Envelope):

                List frame: list of field record frames
                List frame: list of column record frames
                List frame: list of alias column record frames
                List frame: list of extra type information

    In general, a schema extension is optional, and thus this record frame might be empty.
        The interpretation of the information contained therein should be identical as if it was found
        directly at the end of the header. This is necessary when fields have been added during writing.

    Note that the field IDs and physical column IDs given by the serialization order should
        continue from the largest IDs found in the header.

    Note that is it possible to extend existing fields by additional column representations.
        This means that columns of the extension header may point to fields of the regular header.

    In practice, deferred columns only appear in the schema extension record frame.
    """

    fieldDescriptions: ListFrame[FieldDescription]
    """The List Frame of Field Description Record Frames. Part of the RNTuple schema description."""
    columnDescriptions: ListFrame[ColumnDescription]
    """The List Frame of Column Description Record Frames. Part of the RNTuple schema description."""
    aliasColumnDescriptions: ListFrame[AliasColumnDescription]
    """The List Frame of Alias Column Description Record Frames. Part of the RNTuple schema description."""
    extraTypeInformations: ListFrame[ExtraTypeInformation]
    """The List Frame of Extra Type Information Record Frames. Part of the RNTuple schema description."""


@serializable
class FooterEnvelope(REnvelope):
    """A class representing the RNTuple Footer Envelope payload structure."""

    featureFlags: RFeatureFlags
    """The RNTuple Feature Flags (verify this file can be read)"""
    headerChecksum: Annotated[int, Fmt("<Q")]
    """Checksum of the Header Envelope"""
    schemaExtension: SchemaExtension
    """The Schema Extension Record Frame"""
    clusterGroups: ListFrame[ClusterGroup]
    """The List Frame of Cluster Group Record Frames"""

    def get_pagelists(self, fetch_data: DataFetcher) -> list[PageListEnvelope]:
        """Get the RNTuple Page List Envelopes from the Footer Envelope.

        Page List Envelope Links are stored in the Cluster Group Record Frames in the Footer Envelope Payload.
        """

        #### Return the Page List Envelopes
        return [
            g.pagelistLink.read_envelope(fetch_data, PageListEnvelope)
            for g in self.clusterGroups
        ]


ENVELOPE_TYPE_MAP[0x02] = "FooterEnvelope"
