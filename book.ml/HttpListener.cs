using System.Net;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
internal sealed class HttpListener
{
    private const string Prefix = "http://localhost:3000/";
    private readonly System.Net.HttpListener listener = new();
    private static BookSearch bookSearch = new BookSearch("https://www.googleapis.com/books/v1/volumes?q=");

    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        listener.Prefixes.Add(Prefix);
        listener.Start();

        Console.WriteLine($"Listening at {Prefix}");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                HttpListenerContext context = await listener.GetContextAsync()
                    .WaitAsync(cancellationToken);

                await HandleAsync(context, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            listener.Stop();
        }
    }

    private static async Task HandleAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        string? q = context.Request.QueryString["q"];
        string[] books = ParseBooks(q);
        Console.WriteLine("MAIN " + Environment.CurrentManagedThreadId);
        var booksObs = books.ToObservable().SelectMany(x => bookSearch.search(x)).Subscribe(y => Console.WriteLine("hello from " + y.Description));
        
        Console.WriteLine($"Request: {string.Join(", ", books)}");

        context.Response.ContentType = "application/json";
        context.Response.StatusCode = (int)HttpStatusCode.OK;

        await JsonSerializer.SerializeAsync(
            context.Response.OutputStream,
            new { books },
            cancellationToken: cancellationToken);

        context.Response.Close();
    }

    private static string[] ParseBooks(string? query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return [];
        }

        return query.Split(',');
    }
}
