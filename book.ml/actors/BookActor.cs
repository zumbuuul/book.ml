using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;

public class BookActor : UntypedActor
{
    public sealed class StartReading
    {
    }

    public sealed class BookUpdated
    {
        public BookUpdated(Book book)
        {
            this.book = book;
        }

        public Book book { get; }
    }

    public sealed class BookStreamReady
    {
        public BookStreamReady(string bookName)
        {
            BookName = bookName;
        }

        public string BookName { get; }
    }

    public sealed class BookDataReady
    {
        public BookDataReady(string bookName, Book book)
        {
            BookName = bookName;
            Book = book;
        }

        public string BookName { get; }
        public Book Book { get; }
    }

    private ILoggingAdapter Log {get;} = Context.GetLogger();

    private readonly IObservable<Book> bookObservable;
    private readonly string bookName;
    private Book? state;
    private IDisposable? subscription;
    public BookActor(IObservable<Book> book, string name)
    {
        this.bookObservable = book;
        this.bookName = name;
    }

    protected override void OnReceive(object message)
    {
        switch(message)
        {
            case StartReading:
                StartBookStream();
                break;
            case BookUpdated updated:
                this.state = updated.book;
                Print();
                Context.Parent.Tell(new BookDataReady(bookName, updated.book), Self);
                break;
        }
    }

    private void StartBookStream()
    {
        if (subscription != null)
        {
            return;
        }

        Console.WriteLine("START READING");
        var self = Self;
        subscription = bookObservable
            .Where(x => x.Name == this.bookName)
            .Subscribe(
                book => self.Tell(new BookUpdated(book)),
                e => Log.Error(e.Message),
                ()=>Log.Info("STREAM OVER"));

        Context.Parent.Tell(new BookStreamReady(bookName), Self);
    }

    private void Print() => Log.Info("hello from book actor, running on thread " + Environment.CurrentManagedThreadId + " storing book " + this.state?.Description);

    protected override void PostStop()
    {
        Log.Info("UGASEN");
        subscription?.Dispose();
    }

    public static Props Props(IObservable<Book> b, string bookName) => Akka.Actor.Props.Create(() => new BookActor(b, bookName));


}
