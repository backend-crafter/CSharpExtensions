namespace CSharpExtensions.Core.Helpers.Models;

/// <summary>
/// Represents a paginated list of items.
/// </summary>
/// <typeparam name="T">The type of items in the list.</typeparam>
public sealed record PagedList<T>
{
    private IReadOnlyList<T> _items = Array.Empty<T>();
    private int _pageNumber = 1;
    private int _pageSize = 1;
    private long _totalCount;

    public PagedList(IReadOnlyList<T> Items, int PageNumber, int PageSize, long TotalCount)
    {
        ArgumentNullException.ThrowIfNull(Items);
        ArgumentOutOfRangeException.ThrowIfLessThan(PageNumber, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(PageSize, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(TotalCount);

        _items = Items;
        _pageNumber = PageNumber;
        _pageSize = PageSize;
        _totalCount = TotalCount;
    }

    public IReadOnlyList<T> Items
    {
        get => _items;
        init => _items = value ?? throw new ArgumentNullException(nameof(value));
    }

    public int PageNumber
    {
        get => _pageNumber;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _pageNumber = value;
        }
    }

    public int PageSize
    {
        get => _pageSize;
        init
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            _pageSize = value;
        }
    }

    public long TotalCount
    {
        get => _totalCount;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegative(value);
            _totalCount = value;
        }
    }

    /// <summary>
    /// Gets the total number of pages.
    /// </summary>
    public int TotalPages
    {
        get
        {
            var pages = TotalCount / PageSize;
            if (TotalCount % PageSize != 0)
            {
                pages++;
            }

            return pages > int.MaxValue ? int.MaxValue : (int)pages;
        }
    }

    /// <summary>
    /// Gets a value indicating whether there is a previous page.
    /// </summary>
    public bool HasPreviousPage => PageNumber > 1;

    /// <summary>
    /// Gets a value indicating whether there is a next page.
    /// </summary>
    public bool HasNextPage => PageNumber < TotalPages;

    public void Deconstruct(
        out IReadOnlyList<T> Items,
        out int PageNumber,
        out int PageSize,
        out long TotalCount)
    {
        Items = this.Items;
        PageNumber = this.PageNumber;
        PageSize = this.PageSize;
        TotalCount = this.TotalCount;
    }
}
