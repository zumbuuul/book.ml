using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;

public class RequestGroupActor : UntypedActor
{
    private ILoggingAdapter Log {get; } = Context.GetLogger();
    public sealed class BookReceived
    {
        public BookReceived(Book Book)
        {
            this.Book = Book;
        }

        public Book Book { get; }
    }

    private IActorRef replyTo = ActorRefs.Nobody;
    private readonly HashSet<string> books;
    private readonly BookCache kes;
    private readonly IObservable<Book> bookObservable;
    private HashSet<string> missingBooks = new HashSet<string>();
    private IDisposable? missingBooksSubscription;
    private bool requestContinued;
    public RequestGroupActor(HashSet<string> b, BookCache k)
    {
        this.books = b.Select(book => book.Trim()).Where(book => !string.IsNullOrWhiteSpace(book)).ToHashSet();  
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
            case BookReceived received:
                bookArrived(received.Book);
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

        if (missingBooks.Count == 0)
        {
            continueRequest();
            return;
        }

        IActorRef self = Self;
        missingBooksSubscription = bookObservable
            .Where(book => missingBooks.Contains(book.Name))
            .Subscribe(
                book => self.Tell(new BookReceived(book)),
                error => self.Tell(new topicModelingFailed(error.Message)));

        kes.requestMissingBooks(missingBooks);
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
        Context.Stop(Self);
    }

    private void failRequest(string error)
    {
        Log.Error("Request group failed: " + error);
        replyTo.Tell(new Status.Failure(new Exception(error)), Self);
        //Context.Stop(Self);
    }

    private void bookArrived(Book book)
    {
        missingBooks.Remove(book.Name);
        Log.Info("Rx delivered missing book " + book.Name + ". Still missing: " + missingBooks.Count);

        if (missingBooks.Count == 0)
        {
            continueRequest();
        }
    }

    private void continueRequest()
    {
        if (requestContinued)
        {
            return;
        }

        requestContinued = true;
        missingBooksSubscription?.Dispose();
        Log.Info("Continuing request with " + kes.getBooksForTopicModeling(books).Count + " discovered books");
        startTopicModeling();
    }

    protected override void PostStop()
    {
        missingBooksSubscription?.Dispose();
    }

    public static Props Props(HashSet<string> b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroupActor(b, k));
}
