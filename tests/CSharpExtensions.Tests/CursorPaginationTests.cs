using CSharpExtensions.Core.Helpers.Extensions;
using CSharpExtensions.Core.Helpers.Models;

namespace CSharpExtensions.Tests;

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

public sealed class CursorPaginationTests
{
    private record TestItem(int Id, string Name);
    private record MappedItem(string Display);

    [Fact]
    public void ToCursorPagedList_ShouldThrowArgumentNullException_WhenSourceIsNull()
    {
        // Arrange
        IEnumerable<TestItem> source = null!;

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => source.ToCursorPagedList(10, x => x.Id));
        Assert.Equal("source", exception.ParamName);
    }

    [Fact]
    public void ToCursorPagedList_ShouldThrowArgumentNullException_WhenCursorSelectorIsNull()
    {
        // Arrange
        var source = new List<TestItem> { new(1, "A") };

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => source.ToCursorPagedList(10, (Func<TestItem, int>)null!));
        Assert.Equal("cursorSelector", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void ToCursorPagedList_ShouldThrowArgumentOutOfRangeException_WhenPageSizeIsLessThanOrEqualToZero(int pageSize)
    {
        // Arrange
        var source = new List<TestItem> { new(1, "A") };

        // Act & Assert
        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => source.ToCursorPagedList(pageSize, x => x.Id));
        Assert.Equal("pageSize", exception.ParamName);
    }

    [Fact]
    public void ToCursorPagedList_ShouldReturnEmptyList_WhenSourceIsEmpty()
    {
        // Arrange
        var source = Enumerable.Empty<TestItem>();

        // Act
        var result = source.ToCursorPagedList(5, x => x.Id);

        // Assert
        Assert.Empty(result.Items);
        Assert.Equal(5, result.PageSize);
        Assert.False(result.HasMore);
        Assert.Equal(0, result.NextCursor);
    }

    [Fact]
    public void ToCursorPagedList_ShouldNotHaveMore_WhenSourceCountIsLessThanPageSize()
    {
        // Arrange
        var source = new List<TestItem>
        {
            new(1, "A"),
            new(2, "B")
        };

        // Act
        var result = source.ToCursorPagedList(5, x => x.Id);

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal(5, result.PageSize);
        Assert.False(result.HasMore);
        Assert.Equal(0, result.NextCursor);
        Assert.Equal(source[0], result.Items[0]);
        Assert.Equal(source[1], result.Items[1]);
    }

    [Fact]
    public void ToCursorPagedList_ShouldNotHaveMore_WhenSourceCountIsEqualToPageSize()
    {
        // Arrange
        var source = new List<TestItem>
        {
            new(1, "A"),
            new(2, "B")
        };

        // Act
        var result = source.ToCursorPagedList(2, x => x.Id);

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal(2, result.PageSize);
        Assert.False(result.HasMore);
        Assert.Equal(0, result.NextCursor);
    }

    [Fact]
    public void ToCursorPagedList_ShouldHaveMoreAndTruncateItems_WhenSourceCountIsGreaterThanPageSize()
    {
        // Arrange
        var source = new List<TestItem>
        {
            new(1, "A"),
            new(2, "B"),
            new(3, "C") // Extra item representing "has more"
        };

        // Act
        var result = source.ToCursorPagedList(2, x => x.Id);

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal(2, result.PageSize);
        Assert.Equal(new TestItem(1, "A"), result.Items[0]);
        Assert.Equal(new TestItem(2, "B"), result.Items[1]);
        Assert.True(result.HasMore);
        Assert.Equal(2, result.NextCursor); // Cursor of the last item in the returned list (B)
    }

    [Fact]
    public void ToCursorPagedList_ShouldWorkWithNonListEnumerable()
    {
        // Arrange
        IEnumerable<TestItem> GetItems()
        {
            yield return new TestItem(1, "A");
            yield return new TestItem(2, "B");
            yield return new TestItem(3, "C");
        }

        // Act
        var result = GetItems().ToCursorPagedList(2, x => x.Id);

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal(2, result.PageSize);
        Assert.True(result.HasMore);
        Assert.Equal(2, result.NextCursor);
    }

    [Fact]
    public void ToCursorPagedList_ShouldEnumerateOnlyOneItemBeyondPage()
    {
        var enumerated = 0;

        IEnumerable<TestItem> GetItems()
        {
            while (true)
            {
                enumerated++;
                if (enumerated > 3)
                {
                    throw new InvalidOperationException("The source was enumerated past the lookahead item.");
                }

                yield return new TestItem(enumerated, enumerated.ToString());
            }
        }

        var result = GetItems().ToCursorPagedList(2, item => item.Id);

        Assert.Equal(3, enumerated);
        Assert.Equal(2, result.Items.Count);
        Assert.True(result.HasMore);
    }

    [Fact]
    public void ToCursorPagedList_ShouldRejectUnboundedPageSize()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Enumerable.Empty<TestItem>().ToCursorPagedList(int.MaxValue, item => item.Id));
    }

    [Fact]
    public void Map_ShouldThrowArgumentNullException_WhenSelectorIsNull()
    {
        // Arrange
        var pagedList = new CursorPagedList<TestItem, int>(new List<TestItem> { new(1, "A") }, 5, false, default);

        // Act & Assert
        var exception = Assert.Throws<ArgumentNullException>(() => pagedList.Map((Func<TestItem, MappedItem>)null!));
        Assert.Equal("selector", exception.ParamName);
    }

    [Fact]
    public void Map_ShouldTransformItemsAndPreserveMetadata()
    {
        // Arrange
        var items = new List<TestItem>
        {
            new(1, "A"),
            new(2, "B")
        };
        var pagedList = new CursorPagedList<TestItem, int>(items, 5, true, 2);

        // Act
        var result = pagedList.Map(x => new MappedItem($"{x.Id}:{x.Name}"));

        // Assert
        Assert.Equal(2, result.Items.Count);
        Assert.Equal(5, result.PageSize);
        Assert.True(result.HasMore);
        Assert.Equal(2, result.NextCursor);
        Assert.Equal("1:A", result.Items[0].Display);
        Assert.Equal("2:B", result.Items[1].Display);
    }

    [Fact]
    public void Map_ShouldReturnEmpty_WhenListIsEmpty()
    {
        // Arrange
        var pagedList = new CursorPagedList<TestItem, int>(Array.Empty<TestItem>(), 5, false, default);

        // Act
        var result = pagedList.Map(x => new MappedItem(x.Name));

        // Assert
        Assert.Empty(result.Items);
        Assert.Equal(5, result.PageSize);
        Assert.False(result.HasMore);
        Assert.Equal(0, result.NextCursor);
    }

    [Fact]
    public void ReadOnlyListSlice_ShouldValidateArguments()
    {
        // Arrange
        IReadOnlyList<int> source = new List<int> { 1, 2, 3 };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => new ReadOnlyListSlice<int>(null!, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReadOnlyListSlice<int>(source, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new ReadOnlyListSlice<int>(source, 4));
    }

    [Fact]
    public void ReadOnlyListSlice_ShouldExposeCorrectIndexerAndCount()
    {
        // Arrange
        IReadOnlyList<int> source = new List<int> { 10, 20, 30 };
        var slice = new ReadOnlyListSlice<int>(source, 2);

        // Assert
        Assert.Equal(2, slice.Count);
        Assert.Equal(10, slice[0]);
        Assert.Equal(20, slice[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => slice[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => slice[2]);
    }

    [Fact]
    public void ReadOnlyListSlice_ShouldEnumerateCorrectly()
    {
        // Arrange
        IReadOnlyList<int> source = new List<int> { 10, 20, 30 };
        var slice = new ReadOnlyListSlice<int>(source, 2);

        // Act
        var items = new List<int>();
        foreach (var item in slice) // Tests duck-typed struct GetEnumerator
        {
            items.Add(item);
        }

        // Assert
        Assert.Equal(new[] { 10, 20 }, items);
    }

    [Fact]
    public void ReadOnlyListSlice_ShouldSupportInterfaceEnumeration()
    {
        // Arrange
        IReadOnlyList<int> source = new List<int> { 10, 20, 30 };
        var slice = new ReadOnlyListSlice<int>(source, 2);
        var enumerable = (IEnumerable<int>)slice;

        // Act
        var items = enumerable.ToList();

        // Assert
        Assert.Equal(new[] { 10, 20 }, items);
    }
}
