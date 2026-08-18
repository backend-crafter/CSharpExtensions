using CSharpExtensions.Foundation.Helpers.Models;

namespace CSharpExtensions.Foundation.Helpers.Extensions;

/// <summary>
/// Extension methods for cursor-based pagination.
/// </summary>
public static class CursorPaginationExtensions
{
    private const int MaximumPageSize = 100_000;

    /// <summary>
    /// Converts a source collection to a cursor-paginated list by checking for an extra item.
    /// The source collection should be queried with (pageSize + 1) items to accurately determine if more items exist.
    /// </summary>
    /// <typeparam name="T">The type of elements in the source collection.</typeparam>
    /// <typeparam name="TCursor">The type of the cursor.</typeparam>
    /// <param name="source">The source collection of items (typically containing up to pageSize + 1 items).</param>
    /// <param name="pageSize">The requested page size.</param>
    /// <param name="cursorSelector">A delegate to select/build the cursor from the last item of the current page.</param>
    /// <returns>A <see cref="CursorPagedList{T, TCursor}"/> containing the paginated items and the next cursor if applicable.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="source"/> or <paramref name="cursorSelector"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="pageSize"/> is less than or equal to zero.</exception>
    public static CursorPagedList<T, TCursor> ToCursorPagedList<T, TCursor>(
        this IEnumerable<T> source,
        int pageSize,
        Func<T, TCursor> cursorSelector)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(cursorSelector);

        if (pageSize <= 0 || pageSize > MaximumPageSize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(pageSize),
                pageSize,
                $"Page size must be between 1 and {MaximumPageSize}.");
        }

        // Never enumerate an unbounded source. One extra item is sufficient to
        // determine whether another page exists.
        IReadOnlyList<T> list;
        if (source is IReadOnlyList<T> readOnlyList)
        {
            list = readOnlyList;
        }
        else
        {
            var takeCount = pageSize + 1;
            list = source.Take(takeCount).ToList();
        }

        bool hasMore = list.Count > pageSize;
        
        // Avoid copying page elements by wrapping the existing list in a bounded view.
        IReadOnlyList<T> items = hasMore 
            ? new ReadOnlyListSlice<T>(list, pageSize) 
            : list;

        TCursor? nextCursor = default;
        if (hasMore && items.Count > 0)
        {
            T lastItem = items[^1];
            nextCursor = cursorSelector(lastItem);
        }

        return new CursorPagedList<T, TCursor>(items, pageSize, hasMore, nextCursor);
    }
}
