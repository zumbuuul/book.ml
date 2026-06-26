using Akka.Actor;

public class RequestGroup : UntypedActor
{
    private HashSet<String> books;
    private HashSet<String> missingBooks = new HashSet<string>();
    private BookCache kes;
    public RequestGroup(HashSet<String> b, BookCache k)
    {
        this.books = b;  
        this.kes = k; 
    }
    protected override void OnReceive(object message)
    {
        switch(message)
        {
            case "read":
                this.missingBooks = kes.checkCache(books);
                startFetchingMissingBooks();
                startProcessingBooks();
                break;
            
        }
    }

    private void startProcessingBooks()
    {
        //start actors
    }

    private async Task<Book[]> startFetchingMissingBooks()
    {
        var fetchTasks = missingBooks.Select(missing => kes.fetchBook(missing));
        return await Task.WhenAll(fetchTasks);
    }


    public static Props Props(HashSet<String> b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroup(b, k));
}