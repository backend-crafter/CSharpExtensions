namespace CSharpExtensions.Core.Helpers.Models;

/// <summary>
/// Represents a cursor-paginated list of items, optimized for keyset pagination on large datasets.
/// </summary>
/// <typeparam name="T">The type of items in the list.</typeparam>
/// <typeparam name="TCursor">The type of the keyset cursor.</typeparam>
public sealed record CursorPagedList<T, TCursor>
{
    private IReadOnlyList<T> _items = Array.Empty<T>();
    private int _pageSize;
    private bool _hasMore;

    public CursorPagedList(
        IReadOnlyList<T> Items,
        int PageSize,
        bool HasMore,
        TCursor? NextCursor)
    {
        ArgumentNullException.ThrowIfNull(Items);
        ArgumentOutOfRangeException.ThrowIfNegative(PageSize);

        if (PageSize == 0 && Items.Count != 0)
        {
            throw new ArgumentException("A zero page size is valid only for an empty page.", nameof(PageSize));
        }

        if (HasMore && Items.Count == 0)
        {
            throw new ArgumentException("A page cannot indicate more items when it contains no items.", nameof(HasMore));
        }

        _items = Items;
        _pageSize = PageSize;
        _hasMore = HasMore;
        this.NextCursor = NextCursor;
    }

    public IReadOnlyList<T> Items
    {
        get => _items;
        init
        {
            ArgumentNullException.ThrowIfNull(value);
            if (_pageSize == 0 && value.Count != 0)
            {
                throw new ArgumentException("A zero page size is valid only for an empty page.", nameof(value));
            }

            if (_hasMore && value.Count == 0)
            {
                throw new ArgumentException("A page cannot indicate more items when it contains no items.", nameof(value));
            }

            _items = value;
        }
    }

    public int PageSize
    {
        get => _pageSize;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            if (value == 0 && _items.Count != 0)
            {
                throw new ArgumentException("A zero page size is valid only for an empty page.", nameof(value));
            }

            _pageSize = value;
        }
    }

    public bool HasMore
    {
        get => _hasMore;
        init
        {
            if (value && _items.Count == 0)
            {
                throw new ArgumentException("A page cannot indicate more items when it contains no items.", nameof(value));
            }

            _hasMore = value;
        }
    }

    public TCursor? NextCursor { get; init; }

    /// <summary>
    /// Gets an empty cursor-paginated list.
    /// </summary>
    public static CursorPagedList<T, TCursor> Empty { get; } = new(Array.Empty<T>(), 0, false, default);

    /// <summary>
    /// Transforms the items in this list to a different type using the provided selector delegate.
    /// </summary>
    /// <typeparam name="TResult">The destination type.</typeparam>
    /// <param name="selector">A mapping delegate to transform each item.</param>
    /// <returns>A new <see cref="CursorPagedList{TResult, TCursor}"/> with the mapped items.</returns>
    /// <exception class="ArgumentNullException">Thrown when <paramref name="selector"/> is null.</exception>
    public CursorPagedList<TResult, TCursor> Map<TResult>(Func<T, TResult> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);

        if (Items.Count == 0)
        {
            return new CursorPagedList<TResult, TCursor>(
                Array.Empty<TResult>(),
                PageSize,
                false,
                NextCursor);
        }

        var mappedItems = new TResult[Items.Count];
        for (int i = 0; i < Items.Count; i++)
        {
            mappedItems[i] = selector(Items[i]);
        }

        return new CursorPagedList<TResult, TCursor>(mappedItems, PageSize, HasMore, NextCursor);
    }

    public void Deconstruct(
        out IReadOnlyList<T> Items,
        out int PageSize,
        out bool HasMore,
        out TCursor? NextCursor)
    {
        Items = this.Items;
        PageSize = this.PageSize;
        HasMore = this.HasMore;
        NextCursor = this.NextCursor;
    }
}
