using CSharpExtensions.Core.Exceptions.Exceptions;
using CSharpExtensions.Core.Helpers.Extensions;
using Xunit;

namespace CSharpExtensions.Tests;

public class ExceptionTests
{
    [Fact]
    public void GetMessages_ShouldReturnAllInnerMessages()
    {
        // Arrange
        var inner = new Exception("Inner");
        var middle = new Exception("Middle", inner);
        var outer = new Exception("Outer", middle);

        // Act
        var messages = outer.GetMessages();

        // Assert
        Assert.Equal(3, messages.Count);
        Assert.Equal("Outer", messages[0]);
        Assert.Equal("Middle", messages[1]);
        Assert.Equal("Inner", messages[2]);
    }

    [Fact]
    public void GetCleanStackTrace_ShouldFilterLines()
    {
        // Arrange
        Exception ex;
        try
        {
            throw new InvalidOperationException("Fail");
        }
        catch (Exception e)
        {
            ex = e;
        }

        // Act
        var stackTrace = ex.GetCleanStackTrace();

        // Assert
        Assert.NotEmpty(stackTrace);
        // Each line should be cleaned (no "at ... in " prefix)
        Assert.All(stackTrace, line => Assert.DoesNotContain("at ", line));
    }

    [Fact]
    public void BadRequestException_ShouldHaveCorrectProperties()
    {
        var ex = new BadRequestException("Invalid data", "InvalidType", "Invalid Title");
        Assert.Equal("Invalid data", ex.Message);
        Assert.Equal("InvalidType", ex.Type);
        Assert.Equal("Invalid Title", ex.Title);
        Assert.Equal(400, ex.HttpStatusCode);
    }

    [Fact]
    public void NotFoundException_ShouldHave404Code()
    {
        var ex = new NotFoundException("Not found");
        Assert.Equal(404, ex.HttpStatusCode);
        Assert.Equal("NotFoundError", ex.Type);
    }

    [Fact]
    public void InternalServerException_ShouldHave500Code()
    {
        var ex = new InternalServerException("Internal error");
        Assert.Equal(500, ex.HttpStatusCode);
        Assert.Equal("InternalServerError", ex.Type);
    }

    [Fact]
    public void UnauthorizedException_ShouldHave401Code()
    {
        var ex = new UnauthorizedException("Unauthorized");
        Assert.Equal(401, ex.HttpStatusCode);
        Assert.Equal("UnauthorizedError", ex.Type);
    }

    [Fact]
    public void ForbiddenException_ShouldHave403Code()
    {
        var ex = new ForbiddenException("Forbidden");
        Assert.Equal(403, ex.HttpStatusCode);
        Assert.Equal("ForbiddenError", ex.Type);
    }
}
