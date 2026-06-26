using System.Reactive.Linq;
using Akka.Actor;

public class RequestGroupActor : UntypedActor
{
    private sealed record FetchCompleted(Book[] Books);
    private sealed record FetchFailed(Exception Error);

    private HashSet<String> books;
    private HashSet<String> missingBooks = new HashSet<string>();
    private BookCache kes;
    private IObservable<Book> bookObservable;
    public RequestGroupActor(HashSet<String> b, BookCache k)
    {
        this.books = b;  
        this.kes = k;
        this.bookObservable = k.getBooksObservable();

        
    }
    protected override void OnReceive(object message)
    {
        switch(message)
        {
            case "read":
                StartRequest();
                break;
            case FetchCompleted completed:
                finishRequest(completed.Books);
                break;
            case FetchFailed failed:
                Console.WriteLine("Request group failed: " + failed.Error.Message);
                break; 
           
        }
    }

    private void StartRequest()
    {
        this.missingBooks = kes.checkCache(books);
        Console.WriteLine("missing books " + this.missingBooks.Count);

        _ = fetchMissingBooksAndNotifySelf(this.missingBooks.ToArray(), Self);
    }

    private async Task fetchMissingBooksAndNotifySelf(IEnumerable<string> booksToFetch, IActorRef replyTo)
    {
        try
        {
            Book[] fetchedBooks = await startFetchingMissingBooks(booksToFetch);
            replyTo.Tell(new FetchCompleted(fetchedBooks));
        }
        catch (Exception error)
        {
            replyTo.Tell(new FetchFailed(error));
        }
    }

    private void finishRequest(Book[] fetchedBooks)
    {
        Console.WriteLine("finished fetching " + fetchedBooks.Length + " missing books");
        startTopicModeling();
        startProcessingBooks();
    }

    private void startTopicModeling()
    {
        HashSet<Book> discoveredBooks = kes.getBooksForTopicModeling(books);
        IObservable<HashSet<Book>> booksStream = Observable.Return(discoveredBooks);
        IActorRef topicModelActor = Context.ActorOf(Akka.Actor.Props.Create<TopicModelActor>());

        topicModelActor.Tell(new runTopicModeling(booksStream));
    }

    private void startProcessingBooks()
    {
        //start actors
        this.bookObservable = kes.getBooksObservable();
        foreach(String bookName in books)
        {
            Console.WriteLine("created actor for " + bookName);
            var bookActor = Context.ActorOf(BookActor.Props(this.bookObservable, bookName));
            bookActor.Tell("read");
        }
    }

    private async Task<Book[]> startFetchingMissingBooks(IEnumerable<string> booksToFetch)
    {
        var fetchTasks = booksToFetch.Select(missing => kes.fetchBook(missing));
        return await Task.WhenAll(fetchTasks);
    }


    public static Props Props(HashSet<String> b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroupActor(b, k));
}
