using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;

public class RequestGroupActor : UntypedActor
{
    private ILoggingAdapter Log {get; } = Context.GetLogger();
    private IActorRef replyTo = ActorRefs.Nobody;
    private readonly HashSet<string> books;
    private readonly BookCache kes;
    private readonly IObservable<Book> bookObservable;
    private readonly Dictionary<string, IActorRef> bookActorsByName = new();
    private readonly Dictionary<string, Book> collectedBooks = new();
    private HashSet<IActorRef> pendingBookReplies = new();
    private HashSet<string> missingBooks = new();
    private int currentQueryId;
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
            case BookActor.BookDataResponse response:
                handleBookDataResponse(response, Sender);
                break;
            case BookActor.BookDataReady ready:
                bookArrived(ready);
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
        createBookActors();
        queryBookActors();
    }

    private void createBookActors()
    {
        foreach (string book in books)
        {
            IActorRef bookActor = Context.ActorOf(BookActor.Props(bookObservable, book));
            bookActorsByName[book] = bookActor;
            bookActor.Tell(new BookActor.StartReading());
            Log.Info("Created request child actor for " + book);
        }
    }

    private void queryBookActors()
    {
        currentQueryId++;
        collectedBooks.Clear();
        pendingBookReplies = bookActorsByName.Values.ToHashSet();

        if (pendingBookReplies.Count == 0)
        {
            continueRequest();
            return;
        }

        foreach (IActorRef bookActor in pendingBookReplies)
        {
            bookActor.Tell(new BookActor.GetBookData(currentQueryId), Self);
        }
    }

    private void handleBookDataResponse(BookActor.BookDataResponse response, IActorRef sender)
    {
        if (response.QueryId != currentQueryId)
        {
            return;
        }

        pendingBookReplies.Remove(sender);

        if (response.Book != null)
        {
            collectedBooks[response.BookName] = response.Book;
        }

        if (pendingBookReplies.Count == 0)
        {
            completeBookQuery();
        }
    }

    private void completeBookQuery()
    {
        missingBooks = books
            .Where(book => !collectedBooks.ContainsKey(book))
            .ToHashSet();

        Log.Info("Missing books " + missingBooks.Count + " on thread " + Environment.CurrentManagedThreadId);

        if (missingBooks.Count == 0)
        {
            continueRequest();
            return;
        }

        kes.requestMissingBooks(missingBooks);
    }

    private void startTopicModeling(HashSet<Book> discoveredBooks)
    {
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

    private void bookArrived(BookActor.BookDataReady ready)
    {
        if (!missingBooks.Remove(ready.BookName))
        {
            return;
        }

        collectedBooks[ready.BookName] = ready.Book;

        Log.Info("Child actor received missing book " + ready.BookName + ". Still missing: " + missingBooks.Count);

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
        Log.Info("Continuing request with " + collectedBooks.Count + " books collected from child actors");
        startTopicModeling(collectedBooks.Values.ToHashSet());
    }

    public static Props Props(HashSet<string> b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroupActor(b, k));
}
