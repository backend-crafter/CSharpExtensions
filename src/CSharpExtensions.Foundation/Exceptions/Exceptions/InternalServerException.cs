namespace CSharpExtensions.Foundation.Exceptions.Exceptions;

public class InternalServerException : ApiException
{
    public InternalServerException(string message, string type = "InternalServerError", string title = "Server Error") 
        : base(message, type, title, 500) { }

    public InternalServerException(string message, Exception innerException, string type = "InternalServerError", string title = "Server Error") 
        : base(message, type, title, 500, innerException) { }
}
