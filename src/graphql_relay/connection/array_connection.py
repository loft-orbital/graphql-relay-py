from typing import Any, Iterator, List, Tuple, Optional, Sequence, Type

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
    EdgeType,
    EdgeConstructor,
    PageInfo,
    PageInfoConstructor,
)

__all__ = [
    "connection_from_array",
    "connection_from_array_slice",
    "cursor_for_object_in_connection",
    "cursor_to_offset",
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
) -> ConnectionType:
    """Create a connection object from a slice of the result set.

    Note that different from its JavaScript counterpart which expects an array,
    this function accepts any kind of sliceable object. This object represents
    a slice of the full result set. You can optionally pass the start position of the
    slice as `slice start` and the length of the full result set as `array_length`.
    If the `array_slice` does not have a length, you can provide it separately
    in `array_slice_length` as well.

    This function is similar to `connection_from_array`, but is intended for use
    cases where the cardinality of the connection is considered too large
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

    # Possible combinations are:
    # - first and after
    # - last and before

    if first and last:
        raise ValueError("Mixing 'first' and 'last' is not supported.")

    if before and after:
        raise ValueError("Mixing 'before' and 'after' is not supported.")

    if first and before:
        raise ValueError("Mixing 'first' and 'before' is not supported.")

    if last and after:
        raise ValueError("Mixing 'last' and 'after' is not supported.")

    # If the `array_slice_length` is provided, use it as `array_length`.
    if array_slice_length is not None:
        array_length = array_slice_length

    # If `after` is provided, but `first` is not, or if `first` and `last` are not provided at all,
    # calculate `first` by using the `array_length`
    # or fall back to calculating the array length (which can be an expensive operation, hence being the last resort).
    if first is None and (after or last is None):
        if array_length is not None:
            first = array_length
        else:
            array_length = len(array_slice)
            first = array_length

    # If `before` is provided, but `last` is not,
    # calculate `last` by using the `array_length`
    # or fall back to calculating the array length (which can be an expensive operation, hence being the last resort).
    if last is None and before:
        last = array_length

    # If `last` or `before` were provided
    if last is not None:
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

    # If `first` or `after` are provided
    else:
        assert first is not None
        (
            edges,
            has_previous_page,
            has_next_page,
        ) = _handle_first_after(
            array_slice=array_slice,
            array_length=array_length,
            first=first,
            after=after,
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


def _handle_first_after(
    array_slice: SizedSliceable,
    array_length: Optional[int],
    first: int,
    after: Optional[str],
    slice_start: int = 0,
    edge_type: EdgeConstructor = Edge,
) -> Tuple[List[EdgeType], bool, bool]:
    """Handle the `first` and `after` arguments."""
    if first is not None and first < 0:
        raise ValueError("Argument 'first' must be a non-negative integer.")

    # If defined, convert `after` cursor into an offset.
    after_offset: Optional[int] = cursor_to_offset(after) if after else None

    # Calculate the `start_offset`:
    # If `after` is not provided, start at the beginning of the slice.
    # Otherwise, start right past the `after` cursor.
    # If `after` is provided outside of the bounds of the slice,
    # treat it as though it is at the start of the slice.
    start_offset: int = max(
        slice_start, 0 if after_offset is None else after_offset + 1
    )
    if array_length is not None and after_offset is not None:
        if after_offset > array_length:
            start_offset = 0

    # Calculate the `end_offset`:
    # If `first` is not provided, then set `end_offset` to `start_offset`
    # Otherwise, add `first` to `start_offset`.
    # If `end` is larger than the slice, then set it to the slice length.
    end_offset: int = start_offset + (first or 0)
    if array_length is not None:
        if end_offset > array_length:
            end_offset = array_length

    trimmed_slice: SizedSliceable
    has_previous_page: bool
    has_next_page: bool

    # If length of slice is unknown, slice off one more than we will be returning
    if array_length is None:
        intermediate_slice: SizedSliceable = array_slice[
            start_offset - slice_start : end_offset - slice_start + 1
        ]
        # Keep intermediate `intermediate_slice_length` variable to force QuerySet evaluation.
        intermediate_slice_length: int = len(intermediate_slice)

        trimmed_slice = intermediate_slice[: end_offset - start_offset]
        trimmed_slice_length: int = len(trimmed_slice)

        has_next_page = intermediate_slice_length > trimmed_slice_length

    else:
        trimmed_slice = array_slice[
            start_offset - slice_start : end_offset - slice_start
        ]

        first_edge_offset: int = 0
        if after_offset is not None:
            if 0 <= after_offset < array_length:
                first_edge_offset = after_offset + 1
        last_edge_offset: int = array_length - 1
        has_next_page = last_edge_offset - first_edge_offset + 1 > first

    # If the start offset is greater than zero, there is a previous page.
    # However, if the provided `after` cursor is outside the bounds of the slice,
    # enforce that `has_previous_page` is `True`.
    has_previous_page = start_offset > 0
    if array_length is not None and after_offset is not None:
        if after_offset > array_length:
            has_previous_page = True

    edges = [
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
    edge_type: EdgeConstructor = Edge,
) -> Tuple[List[EdgeType], bool, bool]:
    """Handle the `last` and `before` arguments."""

    if last is not None and last < 0:
        raise ValueError("Argument 'last' must be a non-negative integer.")

    # If defined, convert `before` cursor into an offset.
    before_offset: Optional[int] = cursor_to_offset(before) if before else None

    # If the length of the array is not provided, calculate it.
    if array_length is None:
        array_length = len(array_slice)

    # Calculate the `end_offset`:
    # If `before` is provided, use it as `end_offset` (cropping it to the bounds of the slice).
    # Otherwise, the `end_offset` is the end of the slice.
    end_offset: int
    if before_offset is not None and before_offset >= 0:
        end_offset = min(before_offset, array_length)

    else:
        end_offset = array_length

    # Calculate the `start_offset`:
    # `last` is used it to calculate the `start_offset` by subtracting it from the `end_offset`,
    # ensuring that it is greater than or equal to zero, or to the start of slice (whichever is greater).
    start_offset: int = max(end_offset - last, max(slice_start, 0))

    trimmed_slice: SizedSliceable = array_slice[
        start_offset - slice_start : end_offset - slice_start
    ]

    has_previous_page: bool = start_offset > 0

    has_next_page: bool = end_offset < array_length
    if before_offset is not None and before_offset < 0:
        has_next_page = True

    edges = [
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
