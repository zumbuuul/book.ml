using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;

public class RequestGroupActor : UntypedActor
{
    private ILoggingAdapter Log {get; } = Context.GetLogger();
    public sealed class FetchCompleted
    {
        public FetchCompleted(Book[] Books)
        {
            this.Books = Books;
        }

        public Book[] Books { get; }
    }

    public sealed class FetchFailed
    {
        public FetchFailed(Exception Error)
        {
            this.Error = Error;
        }

        public Exception Error { get; }
    }

    private IActorRef replyTo = ActorRefs.Nobody;
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
                replyTo = Sender;
                StartRequest();
                break;
            case FetchCompleted completed:
                continueRequest(completed.Books);
                break;
            case FetchFailed failed:
                failRequest(failed.Error.Message);
                break; 
            case topicModelingCompleted completed:
                completeRequest(completed);
                break;
            case topicModelingFailed failed:
                failRequest(failed.error);
                break;
           
        }
    }

    private void StartRequest()
    {
        this.missingBooks = kes.checkCache(books);
        Log.Info("Missing books " + this.missingBooks.Count + " on thread " + Environment.CurrentManagedThreadId );

        fetchMissingBooksAndNotifySelf(this.missingBooks.ToArray(), Self);
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

    private void continueRequest(Book[] fetchedBooks)
    {
        Log.Info("finished fetching " + fetchedBooks.Length + " missing books");
        startTopicModeling();
        startProcessingBooks();
    }

    private void startTopicModeling()
    {
        HashSet<Book> discoveredBooks = kes.getBooksForTopicModeling(books);
        IObservable<HashSet<Book>> booksStream = Observable.Return(discoveredBooks);
        IActorRef topicModelActor = Context.ActorOf(
            Akka.Actor.Props.Create<TopicModelActor>().WithDispatcher("default-fork-join-dispatcher"));

        topicModelActor.Tell(new runTopicModeling(booksStream), Self);
    }

    private void completeRequest(topicModelingCompleted completed)
    {
        var response = new
        {
            books = completed.results.Select(book => new
            {
                name = book.bookName,
                topics = book.topics.Select(topic => new
                {
                    name = topic.topic,
                    percentage = topic.percentage
                }).ToList()
            }).ToList()
        };

        replyTo.Tell(response, Self);
        //Context.Stop(Self);
    }

    private void failRequest(string error)
    {
        Log.Error("Request group failed: " + error);
        replyTo.Tell(new Status.Failure(new Exception(error)), Self);
        Context.Stop(Self);
    }

    private void startProcessingBooks()
    {
        //start actors
        this.bookObservable = kes.getBooksObservable();
        foreach(String bookName in books)
        {
            Log.Info("this thread created the actor " + Environment.CurrentManagedThreadId);
            Log.Info("created actor for " + bookName);
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
