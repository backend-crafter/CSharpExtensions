namespace CSharpExtensions.Foundation.Railway;

/// <summary>
/// Represents the result of an operation, following the Railway Oriented Programming pattern.
/// Implemented as a readonly record struct so the success discriminator is a value type.
/// Referenced values, errors, strings, and asynchronous operations can still allocate.
/// </summary>
public readonly record struct Result
{
    /// <summary>
    /// Initializes a new instance of the <see cref="Result"/> struct.
    /// </summary>
    /// <param name="isSuccess">Whether the operation was successful.</param>
    /// <param name="error">The error associated with a failure.</param>
    /// <exception cref="InvalidOperationException">Thrown if success and error states are inconsistent.</exception>
    private Result(bool isSuccess, Error error)
    {
        if (isSuccess && !ReferenceEquals(error, Error.None))
        {
            throw new InvalidOperationException("A successful result cannot contain an error.");
        }

        if (!isSuccess && (ReferenceEquals(error, Error.None) || error.HttpStatusCode is < 400 or > 599))
        {
            throw new InvalidOperationException("A failed result must contain a valid HTTP client or server error.");
        }

        IsSuccess = isSuccess;
        _error = error;
    }

    /// <summary>
    /// Gets a value indicating whether the operation was successful.
    /// </summary>
    public bool IsSuccess { get; }

    // Sequential field layout: IsSuccess, then Error.
    private readonly Error? _error;

    /// <summary>
    /// Gets a value indicating whether the operation failed.
    /// </summary>
    public bool IsFailure => !IsSuccess;

    /// <summary>
    /// Gets the error associated with the failure. Returns <see cref="Error.None"/> if successful.
    /// </summary>
    public Error Error => _error ?? Error.Uninitialized;

    /// <summary>
    /// Creates a successful result.
    /// </summary>
    public static Result Success() => new(true, Error.None);

    /// <summary>
    /// Creates a successful result with a value.
    /// </summary>
    public static Result<TValue> Success<TValue>(TValue value) => new(value, true, Error.None);

    /// <summary>
    /// Creates a failed result.
    /// </summary>
    public static Result Failure(Error error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new Result(false, error);
    }

    /// <summary>
    /// Creates a failed result with a specific type.
    /// </summary>
    public static Result<TValue> Failure<TValue>(Error error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return new Result<TValue>(default, false, error);
    }

    /// <summary>
    /// Creates a failed result with a message.
    /// </summary>
    public static Result Failure(string message) => new(false, new Error(message));

    /// <summary>
    /// Creates a failed result with a message for a specific type.
    /// </summary>
    public static Result<TValue> Failure<TValue>(string message) => new(default, false, new Error(message));

    /// <summary>
    /// Creates a result based on the nullability of a value.
    /// </summary>
    public static Result<TValue> Create<TValue>(TValue? value) 
        => value is not null ? Success(value) : Failure<TValue>(new Error($"Expected value of type {typeof(TValue).Name} was null."));

    /// <summary>
    /// Implicitly converts an <see cref="Error"/> to a failed <see cref="Result"/>.
    /// </summary>
    public static implicit operator Result(Error error) => Failure(error);
}
