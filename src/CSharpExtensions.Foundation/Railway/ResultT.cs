namespace CSharpExtensions.Foundation.Railway;

/// <summary>
/// Represents the result of an operation that returns a value.
/// Implemented as a readonly record struct with a value-type success discriminator.
/// </summary>
/// <typeparam name="TValue">The type of the result value.</typeparam>
public readonly record struct Result<TValue>
{
    private readonly TValue? _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="Result{TValue}"/> struct.
    /// </summary>
    internal Result(TValue? value, bool isSuccess, Error error)
    {
        if (isSuccess && !ReferenceEquals(error, Error.None))
        {
            throw new InvalidOperationException("A successful result cannot contain an error.");
        }

        if (!isSuccess && (ReferenceEquals(error, Error.None) || error.HttpStatusCode is < 400 or > 599))
        {
            throw new InvalidOperationException("A failed result must contain a valid HTTP client or server error.");
        }

        _value = value;
        IsSuccess = isSuccess;
        _error = error;
    }

    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// </summary>
    public bool IsSuccess { get; }

    // Sequential field layout: Value, IsSuccess, then Error.
    private readonly Error? _error;

    /// <summary>
    /// Gets a value indicating whether the operation failed.
    /// </summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>
    /// Gets the error associated with the failure.
    /// </summary>
    public Error Error => _error ?? Error.Uninitialized;

    /// <summary>
    /// Gets the value of the result. Throws <see cref="InvalidOperationException"/> if the operation failed.
    /// </summary>
    public TValue Value => IsSuccess ? _value! : throw new InvalidOperationException("Value is not available for a failed result.");

    /// <summary>
    /// Gets the value or the default value for the type if the operation failed.
    /// </summary>
    public TValue? ValueOrDefault => IsSuccess ? _value : default;

    /// <summary>
    /// Implicitly converts a value to a successful <see cref="Result{TValue}"/>.
    /// </summary>
    public static implicit operator Result<TValue>(TValue? value) => Result.Create(value);

    /// <summary>
    /// Implicitly converts an <see cref="Error"/> to a failed <see cref="Result{TValue}"/>.
    /// </summary>
    public static implicit operator Result<TValue>(Error error) => Result.Failure<TValue>(error);

    /// <summary>
    /// Implicitly converts a <see cref="Result{TValue}"/> to a non-generic <see cref="Result"/>.
    /// </summary>
    public static implicit operator Result(Result<TValue> result) 
        => result.IsSuccess ? Result.Success() : Result.Failure(result.Error);
}
