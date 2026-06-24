using Microsoft.Extensions.Configuration;

public class LoadEnvKeys
{
    private String? apiKey;
    private IConfiguration config;
    public LoadEnvKeys(IConfiguration config)
    {
     this.config = config;   
    }

    public String loadKeys()
    {
        if (apiKey == null)
        {
            this.apiKey = this.config["API_KEY"];

            return this.apiKey!;
        }
        else
        {
            return this.apiKey;
        }
    }
}