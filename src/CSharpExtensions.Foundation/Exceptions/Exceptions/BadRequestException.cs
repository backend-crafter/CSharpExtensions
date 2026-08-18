namespace CSharpExtensions.Foundation.Exceptions.Exceptions;

public class BadRequestException : ApiException
{
    public BadRequestException(string message, string type = "BadRequest", string title = "Bad Request") 
        : base(message, type, title, 400) { }

    public BadRequestException(string message, Exception innerException, string type = "BadRequest", string title = "Bad Request") 
        : base(message, type, title, 400, innerException) { }
}
