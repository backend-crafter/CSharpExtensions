namespace CSharpExtensions.Foundation.Exceptions.Exceptions;

public class ForbiddenException : ApiException
{
    public ForbiddenException(string message = "Forbidden") 
        : base(message, "ForbiddenError", "Access denied", 403) { }

    public ForbiddenException(string message, Exception innerException) 
        : base(message, "ForbiddenError", "Access denied", 403, innerException) { }
}
