namespace CSharpExtensions.Core.Exceptions.Exceptions;

public class NotFoundException : ApiException
{
    public NotFoundException(string message = "Not Found") 
        : base(message, "NotFoundError", "Resource not found", 404) { }

    public NotFoundException(string message, Exception innerException) 
        : base(message, "NotFoundError", "Resource not found", 404, innerException) { }
}
