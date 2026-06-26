using System.Reactive.Linq;
using Akka.Actor;

public class BookActor : UntypedActor
{
    private IObservable<Book> bookObservable;
    private String bookName;
    private Book state;
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
                        bookObservable.Where(x => x.Name == this.bookName).Subscribe((book) => {this.state = book;Print();});

                break;
        }
    }

    private void Print() => Console.WriteLine("hello from book actor, running on thread " + Environment.CurrentManagedThreadId + " storing book " + this.state.Description);

    public static Props Props(IObservable<Book> b, String bookName) => Akka.Actor.Props.Create(() => new BookActor(b, bookName));


}