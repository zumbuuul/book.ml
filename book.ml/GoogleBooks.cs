using System.Text.Json.Serialization;

public sealed class GoogleBookResponse
{
    [JsonPropertyName("items")]
    public List<GoogleBook> Items {get;set;} = [];
}

public sealed class GoogleBook
{
    [JsonPropertyName("id")]
    public String? Id {get;set;}

    [JsonPropertyName("volumeInfo")]
    public VolumeInfo? VolumeInfo {get;set;}
}

public sealed class VolumeInfo
{
    [JsonPropertyName("title")]
    public String? Title {get;set;}

    [JsonPropertyName("description")]
    public String? Description {get;set;}
}