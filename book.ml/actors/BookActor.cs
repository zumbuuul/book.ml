using System.Reactive.Linq;
using Akka.Actor;

public class BookActor : UntypedActor
{
    private sealed record BookUpdated(Book book);

    private IObservable<Book> bookObservable;
    private String bookName;
    private Book state;
    private IDisposable? subscription;
    public BookActor(IObservable<Book> book, String name)
    {
        this.bookObservable = book;
        this.bookName = name;
    }

    protected override void OnReceive(object message)
    {
        switch(message)
        {
            case "read":
                subscription = bookObservable
                    .Where(x => x.Name == this.bookName)
                    .Subscribe(book => Self.Tell(new BookUpdated(book)));
                break;
            case BookUpdated updated:
                this.state = updated.book;
                Print();
                break;
        }
    }

    private void Print() => Console.WriteLine("hello from book actor, running on thread " + Environment.CurrentManagedThreadId + " storing book " + this.state.Description);

    protected override void PostStop()
    {
        subscription?.Dispose();
    }

    public static Props Props(IObservable<Book> b, String bookName) => Akka.Actor.Props.Create(() => new BookActor(b, bookName));


}
