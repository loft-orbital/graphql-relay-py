from typing import Any, Iterator, Optional, Sequence, cast

try:
    from typing import Protocol
except ImportError:  # Python < 3.8
    from typing_extensions import Protocol  # type: ignore

from ..utils.base64 import base64, unbase64
from .connection import (
    Connection,
    ConnectionArguments,
    ConnectionConstructor,
    ConnectionCursor,
    ConnectionType,
    Edge,
    EdgeConstructor,
    PageInfo,
    PageInfoConstructor,
)

__all__ = [
    "connection_from_array",
    "connection_from_array_slice",
    "cursor_for_object_in_connection",
    "cursor_to_offset",
    "get_offset_with_default",
    "offset_to_cursor",
    "SizedSliceable",
]


class SizedSliceable(Protocol):
    def __getitem__(self, index: slice) -> Any:
        ...

    def __iter__(self) -> Iterator:
        ...

    def __len__(self) -> int:
        ...


def connection_from_array(
    data: SizedSliceable,
    args: Optional[ConnectionArguments] = None,
    connection_type: ConnectionConstructor = Connection,
    edge_type: EdgeConstructor = Edge,
    page_info_type: PageInfoConstructor = PageInfo,
) -> ConnectionType:
    """Create a connection object from a sequence of objects.

    Note that different from its JavaScript counterpart which expects an array,
    this function accepts any kind of sliceable object with a length.

    Given this `data` object representing the result set, and connection arguments,
    this simple function returns a connection object for use in GraphQL. It uses
    offsets as pagination, so pagination will only work if the data is static.

    The result will use the default types provided in the `connectiontypes` module
    if you don't pass custom types as arguments.
    """
    return connection_from_array_slice(
        data,
        args,
        slice_start=0,
        array_length=len(data),
        connection_type=connection_type,
        edge_type=edge_type,
        page_info_type=page_info_type,
    )


def connection_from_array_slice(
    array_slice: SizedSliceable,
    args: Optional[ConnectionArguments] = None,
    slice_start: int = 0,
    array_length: Optional[int] = None,
    array_slice_length: Optional[int] = None,
    connection_type: ConnectionConstructor = Connection,
    edge_type: EdgeConstructor = Edge,
    page_info_type: PageInfoConstructor = PageInfo,
    max_limit: Optional[int] = None,
) -> ConnectionType:
    """Create a connection object from a slice of the result set.

    Note that different from its JavaScript counterpart which expects an array,
    this function accepts any kind of sliceable object. This object represents
    a slice of the full result set. You need to pass the start position of the
    slice as `slice start` and the length of the full result set as `array_length`.
    If the `array_slice` does not have a length, you need to provide it separately
    in `array_slice_length` as well.

    This function is similar to `connection_from_array`, but is intended for use
    cases where you know the cardinality of the connection, consider it too large
    to materialize the entire result set, and instead wish to pass in only a slice
    of the total result large enough to cover the range specified in `args`.

    If you do not provide a `slice_start`, we assume that the slice starts at
    the beginning of the result set, and if you do not provide an `array_length`,
    we assume that the slice ends at the end of the result set.
    """
    args = args or {}
    before: Optional[str] = args.get("before")
    after: Optional[str] = args.get("after")
    first: Optional[int] = args.get("first")
    last: Optional[int] = args.get("last")
    offset: Optional[int] = args.get("offset")

    if first and last:
        raise ValueError("Mixing 'first' and 'last' is not supported.")

    if before and after:
        raise ValueError("Mixing 'before' and 'after' is not supported.")

    if after and first is None:
        if max_limit is not None:
            first = max_limit
        elif array_length is not None:
            first = array_length
        else:
            raise ValueError(
                "Setting argument 'after' without setting 'first' is not supported."
            )

    if before and last is None:
        if max_limit is not None:
            last = max_limit
        elif array_length is not None:
            last = array_length
        else:
            raise ValueError(
                "Setting argument 'before' without setting 'last' is not supported."
            )

    if first is None and last is None:
        if max_limit is not None:
            first = max_limit
        elif array_length is not None:
            first = array_length
        else:
            raise ValueError("Either 'first' or 'last' must be provided.")

    if offset:
        if after:
            offset += cast(int, cursor_to_offset(after)) + 1
        # input offset starts at 1 while the graphene offset starts at 0
        after = offset_to_cursor(offset - 1)

    if first or after:
        assert first is not None
        (
            edges,
            has_previous_page,
            has_next_page,
        ) = _handle_first_after(
            array_slice=array_slice,
            first=first,
            after=after,
            slice_start=slice_start,
            edge_type=edge_type,
        )

    elif last or before:
        assert last is not None
        (
            edges,
            has_previous_page,
            has_next_page,
        ) = _handle_last_before(
            array_slice=array_slice,
            array_length=array_length,
            last=last,
            before=before,
            slice_start=slice_start,
            edge_type=edge_type,
        )

    else:
        (
            edges,
            has_previous_page,
            has_next_page,
        ) = _handle_first_after(
            array_slice=array_slice,
            first=0,
            after=None,
            slice_start=slice_start,
            edge_type=edge_type,
        )

    first_edge_cursor: Optional[str] = edges[0].cursor if edges else None
    last_edge_cursor: Optional[str] = edges[-1].cursor if edges else None

    return connection_type(
        edges=edges,
        pageInfo=page_info_type(
            startCursor=first_edge_cursor,
            endCursor=last_edge_cursor,
            hasPreviousPage=has_previous_page,
            hasNextPage=has_next_page,
        ),
    )


PREFIX = "arrayconnection:"


def offset_to_cursor(offset: int) -> ConnectionCursor:
    """Create the cursor string from an offset."""
    return base64(f"{PREFIX}{offset}")


def cursor_to_offset(cursor: ConnectionCursor) -> Optional[int]:
    """Extract the offset from the cursor string."""
    try:
        return int(unbase64(cursor)[len(PREFIX) :])
    except ValueError:
        return None


def cursor_for_object_in_connection(
    data: Sequence, obj: Any
) -> Optional[ConnectionCursor]:
    """Return the cursor associated with an object in a sequence.

    This function uses the `index` method of the sequence if it exists,
    otherwise searches the object by iterating via the `__getitem__` method.
    """
    try:
        offset = data.index(obj)
    except AttributeError:
        # data does not have an index method
        offset = 0
        try:
            while True:
                if data[offset] == obj:
                    break
                offset += 1
        except IndexError:
            return None
        else:
            return offset_to_cursor(offset)
    except ValueError:
        return None
    else:
        return offset_to_cursor(offset)


def get_offset_with_default(
    cursor: Optional[ConnectionCursor] = None, default_offset: int = 0
) -> int:
    """Get offset from a given cursor and a default.

    Given an optional cursor and a default offset, return the offset to use;
    if the cursor contains a valid offset, that will be used,
    otherwise it will be the default.
    """
    if not isinstance(cursor, str):
        return default_offset

    offset = cursor_to_offset(cursor)
    return default_offset if offset is None else offset


def _handle_first_after(
    array_slice: SizedSliceable,
    first: int,
    after: Optional[str],
    slice_start: int = 0,
    edge_type: type[Edge] = Edge,
) -> tuple[list[Edge], bool, bool]:
    if first < 0:
        raise ValueError("Argument 'first' must be a non-negative integer.")

    after_offset: Optional[int] = cursor_to_offset(after) if after else None

    start_offset: int = max(
        slice_start, 0 if after_offset is None else after_offset + 1
    )
    end_offset: int = start_offset + first

    # Slice off one more than we will be returning
    intermediate_slice: SizedSliceable = array_slice[
        start_offset - slice_start : end_offset - slice_start + 1
    ]
    intermediate_slice_length: int = len(intermediate_slice)

    trimmed_slice: SizedSliceable = intermediate_slice[: end_offset - start_offset]
    trimmed_slice_length: int = len(trimmed_slice)

    has_previous_page: bool = start_offset > 0
    has_next_page: bool = intermediate_slice_length > trimmed_slice_length

    edges: list[Edge] = [
        edge_type(
            node=node,
            cursor=offset_to_cursor(start_offset + index),
        )
        for index, node in enumerate(trimmed_slice)
    ]

    return (
        edges,
        has_previous_page,
        has_next_page,
    )


def _handle_last_before(
    array_slice: SizedSliceable,
    array_length: Optional[int],
    last: int,
    before: Optional[str],
    slice_start: int = 0,
    edge_type: type[Edge] = Edge,
) -> tuple[list[Edge], bool, bool]:
    if last < 0:
        raise ValueError("Argument 'last' must be a non-negative integer.")

    before_offset: Optional[int] = cursor_to_offset(before) if before else None

    if array_length is None:
        if isinstance(array_slice, list):
            array_length = len(array_slice)
        elif hasattr(array_slice, "count"):
            array_slice.count()
        else:
            raise ValueError("Array slice must have a length or count method.")

    end_offset: int = before_offset if before_offset is not None else array_length
    start_offset: int = max(end_offset - last, max(slice_start, 0))

    # Slice off one more than we will be returning
    intermediate_slice: SizedSliceable = array_slice[
        max(start_offset - slice_start - 1, 0) : end_offset - slice_start
    ]

    trimmed_slice: SizedSliceable = intermediate_slice[0 if start_offset == 0 else 1 :]

    has_previous_page: bool = start_offset > 0
    has_next_page: bool = end_offset < array_length

    edges: list[Edge] = [
        edge_type(
            node=node,
            cursor=offset_to_cursor(start_offset + index),
        )
        for index, node in enumerate(trimmed_slice)
    ]

    return (
        edges,
        has_previous_page,
        has_next_page,
    )
