using System.Reactive.Linq;
using Akka.Actor;

public class RequestGroupActor : UntypedActor
{
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
                this.missingBooks = kes.checkCache(books);
                startFetchingMissingBooks();
                startProcessingBooks();
                break;
            
        }
    }

    private void startProcessingBooks()
    {
        //start actors
        this.bookObservable = kes.getBooksObservable();
        Console.WriteLine("this is books " + books.Count);
        foreach(String bookName in books)
        {
            Console.WriteLine("created actor " + bookName);
            var bookActor = Context.ActorOf(BookActor.Props(this.bookObservable, bookName));
            bookActor.Tell("read");
        }
    }

    private async Task<Book[]> startFetchingMissingBooks()
    {
        var fetchTasks = missingBooks.Select(missing => kes.fetchBook(missing));
        return await Task.WhenAll(fetchTasks);
    }


    public static Props Props(HashSet<String> b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroupActor(b, k));
}