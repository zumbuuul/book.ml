using System.Reactive.Linq;
using Akka.Actor;
using Akka.Event;

public class BookActor : UntypedActor
{
    public sealed class BookUpdated
    {
        public BookUpdated(Book book)
        {
            this.book = book;
        }

        public Book book { get; }
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
            case "read":
                if (subscription is not null)
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
                break;
            case BookUpdated updated:
                this.state = updated.book;
                Print();
                break;
        }
    }

    private void Print() => Log.Info("hello from book actor, running on thread " + Environment.CurrentManagedThreadId + " storing book " + this.state?.Description);

    protected override void PostStop()
    {
        Log.Info("UGASEN");
        subscription?.Dispose();
    }

    public static Props Props(IObservable<Book> b, string bookName) => Akka.Actor.Props.Create(() => new BookActor(b, bookName));


}
