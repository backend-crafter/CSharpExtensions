using System.Collections;

namespace CSharpExtensions.Core.Helpers.Models;

/// <summary>
/// Provides a non-copying, read-only view over the beginning of an existing <see cref="IReadOnlyList{T}"/>.
/// </summary>
/// <typeparam name="T">The type of elements in the slice.</typeparam>
public sealed class ReadOnlyListSlice<T> : IReadOnlyList<T>
{
    private readonly IReadOnlyList<T> _source;
    private readonly int _count;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReadOnlyListSlice{T}"/> class.
    /// </summary>
    /// <param name="source">The underlying list source.</param>
    /// <param name="count">The number of elements to include in the slice from the beginning.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="source"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="count"/> is negative or greater than the source count.</exception>
    public ReadOnlyListSlice(IReadOnlyList<T> source, int count)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (count < 0 || count > source.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(count), count, "Count must be non-negative and less than or equal to the source size.");
        }

        _source = source;
        _count = count;
    }

    /// <inheritdoc />
    public T this[int index]
    {
        get
        {
            if (index < 0 || index >= _count)
            {
                throw new ArgumentOutOfRangeException(nameof(index), index, "Index was out of range.");
            }
            return _source[index];
        }
    }

    /// <inheritdoc />
    public int Count => _count;

    /// <summary>
    /// Returns a struct enumerator that avoids boxing during direct iteration.
    /// </summary>
    /// <returns>A struct-based enumerator.</returns>
    public Enumerator GetEnumerator() => new(this);

    /// <inheritdoc />
    IEnumerator<T> IEnumerable<T>.GetEnumerator() => new Enumerator(this);

    /// <inheritdoc />
    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    /// <summary>
    /// A struct-based enumerator used by direct iteration. Interface-based iteration can box it.
    /// </summary>
    public struct Enumerator : IEnumerator<T>
    {
        private readonly ReadOnlyListSlice<T> _slice;
        private int _index;

        internal Enumerator(ReadOnlyListSlice<T> slice)
        {
            _slice = slice;
            _index = -1;
        }

        /// <inheritdoc />
        public readonly T Current => _slice[_index];

        /// <inheritdoc />
        readonly object? IEnumerator.Current => Current;

        /// <inheritdoc />
        public bool MoveNext()
        {
            _index++;
            return _index < _slice._count;
        }

        /// <inheritdoc />
        public void Reset() => _index = -1;

        /// <inheritdoc />
        public readonly void Dispose() { }
    }
}
