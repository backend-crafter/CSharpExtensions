namespace CSharpExtensions.Core.Exceptions.Exceptions;

public class UnauthorizedException : ApiException
{
    public UnauthorizedException(string message = "Unauthorized") 
        : base(message, "UnauthorizedError", "Authentication failed", 401) { }

    public UnauthorizedException(string message, Exception innerException) 
        : base(message, "UnauthorizedError", "Authentication failed", 401, innerException) { }
}
