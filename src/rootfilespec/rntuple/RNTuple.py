import dataclasses
from collections.abc import Callable
from math import ceil

from rootfilespec.bootstrap import BOOTSTRAP_CONTEXT
from rootfilespec.bootstrap.RAnchor import ROOT3a3aRNTuple
from rootfilespec.bootstrap.streamedobject import Ref, read_streamed_item
from rootfilespec.bootstrap.TList import TList
from rootfilespec.bootstrap.TStreamerInfo import TStreamerInfo
from rootfilespec.rntuple.envelope import RFeatureFlags
from rootfilespec.rntuple.footer import FooterEnvelope
from rootfilespec.rntuple.header import HeaderEnvelope
from rootfilespec.rntuple.pagelist import ClusterSummary, PageListEnvelope
from rootfilespec.rntuple.pagelocations import PageLocations, RPageDescription
from rootfilespec.rntuple.schema import (
    AliasColumnDescription,
    ColumnDescription,
    ColumnType,
    ExtraTypeInformation,
    FieldDescription,
)
from rootfilespec.serializable import (
    BufferContext,
    Locator,
    ReadBuffer,
    ROOTSerializable,
)


@dataclasses.dataclass
class SchemaDescription:
    """A class representing the full schema description of an RNTuple.
    It is a combination of the schema description from the header envelope
    and the schema extension from the footer envelope.
    """

    fieldDescriptions: list[FieldDescription]
    """The full list of field descriptions."""
    columnDescriptions: list[ColumnDescription]
    """The full list of column descriptions."""
    aliasColumnDescriptions: list[AliasColumnDescription]
    """The full list of alias column descriptions."""
    extraTypeInformations: list[ExtraTypeInformation]
    """The full list of extra type information."""

    @classmethod
    def from_envelopes(
        cls, headerEnvelope: HeaderEnvelope, footerEnvelope: FooterEnvelope
    ) -> "SchemaDescription":
        """Creates a SchemaDescription from the header and footer envelopes."""
        # Combine field descriptions
        fieldDescriptions = (
            headerEnvelope.fieldDescriptions.items
            + footerEnvelope.schemaExtension.fieldDescriptions.items
        )

        # Combine column descriptions
        columnDescriptions = (
            headerEnvelope.columnDescriptions.items
            + footerEnvelope.schemaExtension.columnDescriptions.items
        )

        # Combine alias column descriptions
        aliasColumnDescriptions = (
            headerEnvelope.aliasColumnDescriptions.items
            + footerEnvelope.schemaExtension.aliasColumnDescriptions.items
        )

        # Combine extra type information
        extraTypeInformations = (
            headerEnvelope.extraTypeInformations.items
            + footerEnvelope.schemaExtension.extraTypeInformations.items
        )

        return cls(
            fieldDescriptions,
            columnDescriptions,
            aliasColumnDescriptions,
            extraTypeInformations,
        )

    def field_chain(self, field_id: int) -> list[int]:
        """The IDs of a field and of its ancestors, from the field up to its top-level field

        A field ID is the field's position in the combined list (header first,
        then the footer's schema extension). A top-level field names itself as
        its parent, which ends the walk. Raises on an out-of-range ID or a cycle.
        """
        fields = self.fieldDescriptions
        chain: list[int] = []
        fid = field_id
        while True:
            if not 0 <= fid < len(fields):
                msg = f"Field {field_id}: parent chain reaches field ID {fid}, of {len(fields)} fields"
                raise ValueError(msg)
            if fid in chain:
                msg = (
                    f"Field {field_id}: the parent chain has a cycle at field ID {fid}"
                )
                raise ValueError(msg)
            chain.append(fid)
            if fields[fid].fParentFieldID == fid:
                return chain
            fid = fields[fid].fParentFieldID

    def field_path(self, field_id: int) -> bytes:
        """The qualified name of a field: its ancestors' names and its own, joined by "."

        "." is forbidden in field names (spec, *Naming specification*), so the
        result is unambiguous. Names are bytes, as stored.
        """
        chain = self.field_chain(field_id)
        return b".".join(self.fieldDescriptions[fid].fFieldName for fid in chain[::-1])


@dataclasses.dataclass
class InterpretablePage:
    """A class representing an interpretable page description.
    It provides the page description, uncompressed size, and column type.
    """

    pageDescription: RPageDescription
    """The RPageDescription object representing the page."""
    uncompressedSize: int
    """The uncompressed size of the page, in bytes."""
    columnType: ColumnType
    """The type of the column this page belongs to, e.g. kInt32, kFloat64, etc."""
    clusterID: int
    """The RNTuple-wide ID of the page's cluster, continuous across cluster groups."""
    columnID: int
    """The ID of the page's physical column (its position in the combined column list)."""
    fieldID: int
    """The ID of the field the column belongs to (``ColumnDescription.fFieldID``)."""
    fieldDescription: FieldDescription
    """The description of that field."""
    fieldPath: bytes
    """The field's qualified name (see ``SchemaDescription.field_path``)."""


@dataclasses.dataclass
class InterpretableColumn:
    """One physical column in one cluster: its element range and its pages

    Enough on its own to read the column's elements in the cluster. The element
    range is the one ROOT's reader builds (``CommitSuppressedColumnRanges`` and
    ``AddExtendedColumnRanges``, ``tree/ntuple/src/RNTupleDescriptor.cxx:880``
    and ``:920`` at 6.40.04), which is what the spec asks of a reader
    (*Suppressed Columns*; *Column Description* for deferred columns).
    """

    clusterID: int
    """The RNTuple-wide ID of the cluster, continuous across cluster groups."""
    columnID: int
    """The ID of the physical column (its position in the combined column list)."""
    columnDescription: ColumnDescription
    """The description of the column."""
    fieldID: int
    """The ID of the field the column belongs to (``ColumnDescription.fFieldID``)."""
    fieldDescription: FieldDescription
    """The description of that field."""
    fieldPath: bytes
    """The field's qualified name (see ``SchemaDescription.field_path``)."""
    pageLocations: PageLocations[RPageDescription] | None
    """The column's entry in the cluster's page list, as stored.

    ``None`` if the page list has no entry for the column: a cluster committed
    before the model was extended lists only the columns that existed then
    (ROOT's ``SerializePageList``, ``tree/ntuple/src/RNTupleSerialize.cxx:1693``
    at 6.40.04)."""
    suppressed: bool
    """Whether the column is a secondary representation, inactive in this cluster.

    A suppressed column has no pages, and its element range is that of the
    corresponding column of the field's active representation. A column that
    the cluster predates is suppressed if it is deferred and suppressed
    (negative ``fFirstElementIndex``)."""
    firstElementIndex: int
    """The index, within the column, of its first element in this cluster."""
    nElements: int
    """The number of elements of the column in this cluster."""
    nZeroElements: int
    """The number of leading elements that have no page on disk and read as zeros.

    Non-zero only for a deferred column, in the clusters up to its first stored
    element: a reader yields zero bytes for them (spec, *Column Description*).
    The pages hold the remaining ``nElements - nZeroElements``."""
    pages: list[InterpretablePage]
    """The column's pages on disk in this cluster, in order."""


@dataclasses.dataclass
class RNTuple:
    """A class representing an RNTuple."""

    headerEnvelope: HeaderEnvelope
    footerEnvelope: FooterEnvelope
    pagelistEnvelopes: list[PageListEnvelope]

    @classmethod
    def from_anchor(
        cls,
        anchor: ROOT3a3aRNTuple,
        fetch_data: Callable[[Locator[ROOTSerializable]], ReadBuffer],
    ) -> "RNTuple":
        """Reads the RNTuple from the given anchor."""
        headerEnvelope = anchor.get_header(fetch_data)
        footerEnvelope = anchor.get_footer(fetch_data)

        # Verify header checksum in footer
        if footerEnvelope.headerChecksum != headerEnvelope.checksum:
            msg = f"Header checksum mismatch: {footerEnvelope.headerChecksum} != {headerEnvelope.checksum}"
            raise ValueError(msg)
        pagelistEnvelopes = footerEnvelope.get_pagelists(fetch_data)

        # Verify header checksum in each PageListEnvelope
        for pagelistEnvelope in pagelistEnvelopes:
            if pagelistEnvelope.headerChecksum != headerEnvelope.checksum:
                msg = f"PageListEnvelope header checksum mismatch: {pagelistEnvelope.headerChecksum} != {headerEnvelope.checksum}"
                raise ValueError(msg)

        return cls(headerEnvelope, footerEnvelope, pagelistEnvelopes)

    @property
    def featureFlags(self) -> RFeatureFlags:
        """Returns the logical or of the feature flags from the header and footer envelopes."""
        return self.headerEnvelope.featureFlags | self.footerEnvelope.featureFlags

    @property
    def schemaDescription(self) -> SchemaDescription:
        """Returns the full schema description, from the header envelope but including footer information."""
        return SchemaDescription.from_envelopes(
            self.headerEnvelope, self.footerEnvelope
        )

    def streamer_infos(self) -> dict[bytes, TStreamerInfo]:
        """The TStreamerInfo of each class that streamed (role 0x04) fields need, by class name

        Decoded from the content of every extra type information record with content
        identifier 0 (ROOT writes one, in the footer's schema extension: root-io-spec
        ERRATA 10), whose fContent keeps the bytes. Records with other identifiers are
        ignored, as the spec asks. The content is a TList written as if through a
        pointer (RNTupleSerializer::SerializeStreamerInfos), with nothing after it.
        """
        infos: dict[bytes, TStreamerInfo] = {}
        for record in self.schemaDescription.extraTypeInformations:
            if record.fContentIdentifier != 0:  # kStreamerInfo
                continue
            buffer = ReadBuffer(
                memoryview(record.fContent),
                0,
                BOOTSTRAP_CONTEXT,
                BufferContext(abspos=None),
            )
            streamed, rest = read_streamed_item(buffer)
            if not isinstance(streamed, TList) or rest:
                msg = f"Expected the streamer info content to be one TList, got {streamed!r} and {len(rest)} more bytes"
                raise ValueError(msg)
            for item in streamed.items:
                # A pointee comes back bare or as a Ref; always a Ref after #105
                info = item.obj if isinstance(item, Ref) else item
                if not isinstance(info, TStreamerInfo):
                    msg = f"Expected a TStreamerInfo in the streamer info content, got {info!r}"
                    raise ValueError(msg)
                if info.fName in infos:
                    msg = f"Two TStreamerInfo for class {info.fName!r} in the streamer info content"
                    raise ValueError(msg)
                infos[info.fName] = info
        return infos

    @property
    def firstClusterIDs(self) -> list[int]:
        """The ID of the first cluster of each cluster group, in footer order

        Cluster IDs continue across cluster groups: a page list's clusters start
        at the number of clusters in all earlier groups (spec, *Page List
        Envelope*). ``pagelistEnvelopes`` are in the same order as the groups.
        """
        first, out = 0, []
        for group in self.footerEnvelope.clusterGroups:
            out.append(first)
            first += group.fNClusters
        return out

    def get_extended_page_descriptions(
        self,
    ) -> list[list[list[InterpretableColumn]]]:
        """Every column of every cluster, by page-list envelope and cluster.

        Each cluster has one entry per physical column, in column-ID order, so a
        position is the column ID (spec, *Page Locations*). Each entry is enough
        on its own to read the column in that cluster; see
        ``InterpretableColumn``.

        Raises ``ValueError`` where the page lists contradict the footer or the
        schema.
        """
        schema = self.schemaDescription
        nColumns = len(schema.columnDescriptions)
        nHeaderColumns = len(self.headerEnvelope.columnDescriptions.items)
        groups = self.footerEnvelope.clusterGroups
        if len(self.pagelistEnvelopes) != len(groups):
            msg = f"{len(self.pagelistEnvelopes)} page lists for {len(groups)} cluster groups"
            raise ValueError(msg)
        context = _ColumnContext(schema)
        envelopes: list[list[list[InterpretableColumn]]] = []
        # The cluster that lists the most columns so far, and how many: only a
        # model extension adds columns, so no later cluster lists fewer
        widestClusterID, widest = 0, 0
        for pagelistEnvelope, group, firstClusterID in zip(
            self.pagelistEnvelopes, groups, self.firstClusterIDs, strict=True
        ):
            pageLocations = pagelistEnvelope.pageLocations.items
            summaries = pagelistEnvelope.clusterSummaries.items
            if not len(pageLocations) == len(summaries) == group.fNClusters:
                msg = (
                    f"Page list of cluster group starting at cluster {firstClusterID} "
                    f"has {len(summaries)} cluster summaries and page locations for "
                    f"{len(pageLocations)} clusters, the group says {group.fNClusters}"
                )
                raise ValueError(msg)
            clusters: list[list[InterpretableColumn]] = []
            for index, (columnlist, summary) in enumerate(
                zip(pageLocations, summaries, strict=True)
            ):
                clusterID = firstClusterID + index
                listed = columnlist.items
                if not nHeaderColumns <= len(listed) <= nColumns:
                    msg = (
                        f"Cluster {clusterID} lists {len(listed)} columns; the "
                        f"schema has {nHeaderColumns} in the header and "
                        f"{nColumns} in all"
                    )
                    raise ValueError(msg)
                if len(listed) < widest:
                    msg = (
                        f"Cluster {clusterID} lists {len(listed)} columns, fewer "
                        f"than the {widest} of cluster {widestClusterID} before it"
                    )
                    raise ValueError(msg)
                if len(listed) > widest:
                    widestClusterID, widest = clusterID, len(listed)
                clusters.append(context.cluster(clusterID, summary, listed))
            envelopes.append(clusters)
        return envelopes


class _ColumnContext:
    """What ``get_extended_page_descriptions`` needs from the schema, computed once"""

    def __init__(self, schema: SchemaDescription):
        self.schema = schema
        # The columns of a field's representations correspond by their index
        # within the representation, which ROOT counts in column-ID order
        # (tree/ntuple/src/RNTupleSerialize.cxx:1511 at 6.40.04). Each list is in
        # column-ID order, so its first column is the first representation's.
        self.corresponding: list[list[int]] = []
        groups: dict[tuple[int, int], list[int]] = {}
        counts: dict[tuple[int, int], int] = {}
        for columnID, column in enumerate(schema.columnDescriptions):
            representation = (column.fFieldID, column.fRepresentationIndex)
            index = counts.get(representation, 0)
            counts[representation] = index + 1
            group = groups.setdefault((column.fFieldID, index), [])
            group.append(columnID)
            self.corresponding.append(group)
        self.paths: dict[int, bytes] = {}

    def path(self, fieldID: int) -> bytes:
        if fieldID not in self.paths:
            self.paths[fieldID] = self.schema.field_path(fieldID)
        return self.paths[fieldID]

    def deferred_range(self, columnID: int, summary: ClusterSummary) -> tuple[int, int]:
        """The element range in a cluster of a deferred column of the first representation

        The column holds a fixed number of elements per entry: the product of
        the array sizes of its field and its ancestors. The spec allows an
        unsuppressed deferred column only where no ancestor is a collection or a
        variant (*Column Description*); otherwise the range cannot be computed.
        """
        fields = self.schema.fieldDescriptions
        fieldID = self.schema.columnDescriptions[columnID].fFieldID
        chain = self.schema.field_chain(fieldID)
        for fid in chain[1:]:
            if fields[fid].fStructuralRole in (0x01, 0x03):  # collection, variant
                msg = (
                    f"Column {columnID} is deferred, but its field "
                    f"{self.path(fieldID)!r} is inside the collection or variant "
                    f"{self.path(fid)!r}"
                )
                raise ValueError(msg)
        repetitions = 1
        for fid in chain:
            repetitions *= max(fields[fid].fArraySize or 0, 1)
        return (
            summary.fFirstEntryNumber * repetitions,
            summary.fNEntries * repetitions,
        )

    def cluster(
        self,
        clusterID: int,
        summary: ClusterSummary,
        listed: list[PageLocations[RPageDescription]],
    ) -> list[InterpretableColumn]:
        """The columns of one cluster, built in the three steps of ROOT's reader"""
        columns = self.schema.columnDescriptions
        locations: list[PageLocations[RPageDescription] | None] = [
            *listed,
            *[None] * (len(columns) - len(listed)),
        ]

        # 1. The page list: an unsuppressed column starts at its element offset
        # and holds its pages' elements; a column the cluster predates holds none
        suppressed: list[bool] = []
        ranges: list[tuple[int, int] | None] = []
        for columnID, (location, column) in enumerate(
            zip(locations, columns, strict=True)
        ):
            if location is None:
                suppressed.append((column.fFirstElementIndex or 0) < 0)
                ranges.append((0, 0))
            elif location.elementoffset < 0:
                if location.items:
                    msg = (
                        f"Cluster {clusterID}: column {columnID} is suppressed "
                        f"but has {len(location.items)} pages"
                    )
                    raise ValueError(msg)
                suppressed.append(True)
                ranges.append(None)
            else:
                suppressed.append(False)
                nStored = sum(page.n_elements for page in location.items)
                ranges.append((location.elementoffset, nStored))

        # 2. A suppressed column takes the range of the corresponding column of
        # the field's active representation (spec, *Suppressed Columns*)
        for columnID, column in enumerate(columns):
            if ranges[columnID] is not None:
                continue
            for other in self.corresponding[columnID]:
                if (
                    columns[other].fRepresentationIndex != column.fRepresentationIndex
                    and locations[other] is not None
                    and not suppressed[other]
                ):
                    ranges[columnID] = ranges[other]
                    break
            else:
                msg = (
                    f"Cluster {clusterID}: column {columnID} is suppressed, and no "
                    f"other representation of field {self.path(column.fFieldID)!r} "
                    "is active"
                )
                raise ValueError(msg)

        # 3. A deferred column covers the whole cluster, the elements before its
        # first stored one being zeros; a later representation copies the range
        # of the first
        for columnID, column in enumerate(columns):
            if not column.fFirstElementIndex:
                continue
            first = self.corresponding[columnID][0]
            if column.fRepresentationIndex == 0:
                ranges[columnID] = self.deferred_range(columnID, summary)
            elif first != columnID:
                ranges[columnID] = ranges[first]
            else:
                msg = (
                    f"Column {columnID} is deferred and of representation "
                    f"{column.fRepresentationIndex}, but field "
                    f"{self.path(column.fFieldID)!r} has no earlier representation"
                )
                raise ValueError(msg)

        out: list[InterpretableColumn] = []
        for columnID, (location, column) in enumerate(
            zip(locations, columns, strict=True)
        ):
            range_ = ranges[columnID]
            assert range_ is not None
            firstElementIndex, nElements = range_
            descriptions: list[RPageDescription] = []
            nZeroElements = 0
            if location is not None and not suppressed[columnID]:
                descriptions = location.items
                nStored = sum(page.n_elements for page in descriptions)
                nZeroElements = nElements - nStored
                if (
                    nZeroElements < 0
                    or location.elementoffset != firstElementIndex + nZeroElements
                ):
                    msg = (
                        f"Cluster {clusterID}: column {columnID} has elements "
                        f"{firstElementIndex} to {firstElementIndex + nElements}, "
                        f"but its pages hold {nStored} elements from "
                        f"{location.elementoffset}"
                    )
                    raise ValueError(msg)
            elif not suppressed[columnID]:
                nZeroElements = nElements
            fieldID = column.fFieldID
            fieldDescription = self.schema.fieldDescriptions[fieldID]
            fieldPath = self.path(fieldID)
            out.append(
                InterpretableColumn(
                    clusterID=clusterID,
                    columnID=columnID,
                    columnDescription=column,
                    fieldID=fieldID,
                    fieldDescription=fieldDescription,
                    fieldPath=fieldPath,
                    pageLocations=location,
                    suppressed=suppressed[columnID],
                    firstElementIndex=firstElementIndex,
                    nElements=nElements,
                    nZeroElements=nZeroElements,
                    pages=[
                        InterpretablePage(
                            pageDescription=page_description,
                            uncompressedSize=ceil(
                                page_description.n_elements * column.fBitsOnStorage / 8
                            ),  # Convert bits to bytes
                            columnType=column.fColumnType,
                            clusterID=clusterID,
                            columnID=columnID,
                            fieldID=fieldID,
                            fieldDescription=fieldDescription,
                            fieldPath=fieldPath,
                        )
                        for page_description in descriptions
                    ],
                )
            )
        return out
