using Microsoft.Extensions.Configuration;

internal class Service
{
    private readonly string? apiKey;

    public Service(IConfiguration configuration)
    {
        this.apiKey = configuration["API_KEY"];

        Console.WriteLine(apiKey ?? "null");

    }
}