namespace CSharpExtensions.Core.Json.Enums;

/// <summary>
/// Specifies how to handle arrays during a JSON merge operation.
/// </summary>
public enum JsonMergeHandling
{
    /// <summary>
    /// The source array replaces the target array. This is the default behavior.
    /// </summary>
    Replace = 0,

    /// <summary>
    /// Elements from the source array are appended to the end of the target array.
    /// </summary>
    Concat = 1,

    /// <summary>
    /// Only unique elements from the source array (that do not already exist in the target array) are added.
    /// Comparison is performed by the JSON value.
    /// </summary>
    Union = 2,

    /// <summary>
    /// Merges elements by index. Target[0] is merged with Source[0], Target[1] with Source[1], and so on.
    /// </summary>
    Merge = 3
}
