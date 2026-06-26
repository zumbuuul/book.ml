using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;

public class BookCache
{
    private IObservable<long> clockObservable = Observable.Interval(TimeSpan.FromSeconds(5));

    private IObservable<Book> booksObservable = Observable.Empty<Book>();
    private BookSearch bookSearch = new BookSearch("https://www.googleapis.com/books/v1/volumes?q=");
    private ConcurrentDictionary<String, Book> bookDiscovery = new ConcurrentDictionary<String, Book>(); 

    public BookCache()
    {
        clockObservable.Subscribe( async (x) => {Console.WriteLine("clock produced value " + x); await fetchAllBooks();});
    }

    public HashSet<String> checkCache(HashSet<String> books)
    {
        HashSet<String> undiscoveredBooks = new HashSet<String>();

        foreach(String book in books)
        {
            if(!bookDiscovery.ContainsKey(book))
                undiscoveredBooks.Add(book); 
                
            
        }  

        return undiscoveredBooks; 
    }

    public async Task fetchAllBooks()
    {
        this.booksObservable = Observable.FromAsync(()=>fetchBooksAsync()).SelectMany(books => books.ToObservable());
    }

    private async Task<Book[]> fetchBooksAsync()
    {
        var keys = bookDiscovery.Keys;
        var bookTasks = keys.Select(key => bookSearch.search(key));
        return await Task.WhenAll(bookTasks);
    }

    public async Task<Book> fetchBook(String missingBook)
    {
        var book = await bookSearch.search(missingBook);
        bookDiscovery.AddOrUpdate(missingBook, book, (k,v) => v);
        return book;
    }

    public IObservable<Book> getBooksObservable () => this.booksObservable;
}