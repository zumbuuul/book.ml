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
        String desc = "";
        using var client = new HttpClient();
        try{
        GoogleBookResponse? response = await client.GetFromJsonAsync<GoogleBookResponse>(searchEndpoint);
        foreach(GoogleBook res in response!.Items)
        {
            if(res.VolumeInfo!.Description != null)
            {
            desc = res.VolumeInfo!.Description; 
            break;
            }
        }

        if(desc == "")
            {
                throw new Exception("None of the google books have a description, please choose a different book.");
            }
        

        }
        catch(Exception e)
        {
            Console.Write(e.Message);
        }
        finally
        {
            
        }
        return new Book(endpoint, desc, searchEndpoint);
    }


}