using System.IO;
using System.Linq;
using System.Xml.Linq;
using Xunit;

namespace CSharpExtensions.Tests;

public class BuildMetadataTests
{
    [Fact]
    public void AspNetBuildMetadataTarget_ShouldRunBeforeAssemblyInfoIsGenerated()
    {
        var propsPath = FindRepositoryFile(
            "src",
            "CSharpExtensions.AspNetCore",
            "build",
            "CSharpExtensions.AspNetCore.props");
        var document = XDocument.Load(propsPath);
        var target = document.Root?
            .Elements("Target")
            .Single(element => (string?)element.Attribute("Name") == "SetCSharpExtensionsBuildMetadata");

        Assert.NotNull(target);
        Assert.Equal("CoreGenerateAssemblyInfo", (string?)target.Attribute("BeforeTargets"));
    }

    private static string FindRepositoryFile(params string[] relativePathSegments)
    {
        var candidates = new[] { AppContext.BaseDirectory, Directory.GetCurrentDirectory() };

        foreach (var candidate in candidates)
        {
            var directory = new DirectoryInfo(candidate);
            while (directory is not null)
            {
                var path = Path.Combine(new[] { directory.FullName }.Concat(relativePathSegments).ToArray());
                if (File.Exists(path))
                {
                    return path;
                }

                directory = directory.Parent;
            }
        }

        throw new FileNotFoundException("The ASP.NET Core build metadata props file was not found.");
    }
}
