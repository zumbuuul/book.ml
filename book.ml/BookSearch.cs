using System.Net.Http.Json;
using Microsoft.Extensions.Configuration;

public class BookSearch
{
    public readonly String URL;
    public readonly LoadEnvKeys keys;
    public BookSearch(String u)
    {
        this.URL= u;   
        IConfiguration configuration = new ConfigurationBuilder()
				.AddUserSecrets(typeof(BookSearch).Assembly)
				.Build();
        keys = new LoadEnvKeys(configuration);
    }




    public async Task<Book> search(String endpoint)
    {
        String searchEndpoint = this.URL + endpoint + "&key=" + keys.loadKeys();
        using var client = new HttpClient();
        GoogleBookResponse? response = await client.GetFromJsonAsync<GoogleBookResponse>(searchEndpoint);
        foreach(GoogleBook res in response!.Items)
        {
            if(res.VolumeInfo!.Description != null)
            Console.WriteLine(res.VolumeInfo.Description);   
        }
        Console.WriteLine("final endpoint: " + searchEndpoint);
        return new Book(endpoint, "test", "test2");
    }


}