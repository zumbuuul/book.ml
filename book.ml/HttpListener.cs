using System.Net;
using System.Reactive.Linq;
using System.Text.Json;
using Akka.Actor;
using Akka.Configuration;
using Akka.Pattern;
internal sealed class HttpListener
{
    private const string Prefix = "http://localhost:3000/";
    private static readonly Config ActorConfig = ConfigurationFactory.ParseString(@"
default-fork-join-dispatcher {
  type = ForkJoinDispatcher
  throughput = 30
  dedicated-thread-pool {
      thread-count = 3
      deadlock-timeout = 3s
      threadtype = background
  }
}");

    private readonly ActorSystem sistem = ActorSystem.Create("knjige", ActorConfig);
    private readonly BookCache kes = new BookCache();
    private readonly IActorRef requestManager;
    private readonly System.Net.HttpListener listener = new();

    public HttpListener()
    {
        requestManager = sistem.ActorOf(RequestManagerActor.Props(kes), "request-manager");
    }
    

 

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

                HandleAsync(context, cancellationToken);
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

    private async Task HandleAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
     
        string? q = context.Request.QueryString["q"];
        HashSet<string> books = ParseBooks(q);
        Console.WriteLine("MAIN " + Environment.CurrentManagedThreadId);
        Console.WriteLine($"Request: {string.Join(", ", books)}");

        context.Response.ContentType = "application/json";

        try
        {
            object response = await requestManager.Ask<object>(
                new ProcessBookRequest(books),
                TimeSpan.FromSeconds(10));

            context.Response.StatusCode = (int)HttpStatusCode.OK;

            await JsonSerializer.SerializeAsync(
                context.Response.OutputStream,
                response,
                cancellationToken: cancellationToken);
        }
        catch (Exception error)
        {
            context.Response.StatusCode = (int)HttpStatusCode.InternalServerError;

            await JsonSerializer.SerializeAsync(
                context.Response.OutputStream,
                new { error = error.Message },
                cancellationToken: cancellationToken);
        }

        context.Response.Close();
    }

    private static HashSet<string> ParseBooks(string? query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return [];
        }

        return query.Split(',').ToHashSet();
    }
}
