namespace CSharpExtensions.Foundation.Exceptions.Exceptions;

/// <summary>
/// Base exception for API tools.
/// </summary>
public abstract class ApiException : Exception
{
    protected ApiException(string message, string type, string title, int httpStatusCode) : base(message)
    {
        Type = type;
        Title = title;
        HttpStatusCode = httpStatusCode;
        Timestamp = DateTime.UtcNow;
    }

    protected ApiException(string message, string type, string title, int httpStatusCode, Exception innerException) 
        : base(message, innerException)
    {
        Type = type;
        Title = title;
        HttpStatusCode = httpStatusCode;
        Timestamp = DateTime.UtcNow;
    }

    public string Title { get; }
    public string Type { get; }
    public int HttpStatusCode { get; }
    public DateTime Timestamp { get; }
}
