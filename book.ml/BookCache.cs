using System.Collections.Concurrent;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reflection;
using System.Reflection.Metadata.Ecma335;

public class BookCache
{
    private IObservable<Book> bookObservable;

    private BookSearch bookSearch = new BookSearch("https://www.googleapis.com/books/v1/volumes?q=");
    private ConcurrentDictionary<String, Book> bookDiscovery = new ConcurrentDictionary<String, Book>(); 

    public BookCache()
    {
        Console.WriteLine("cache thread " + Environment.CurrentManagedThreadId);
        bookObservable = Observable.Timer(TimeSpan.Zero, TimeSpan.FromSeconds(5))
        .SelectMany(_ => Observable.FromAsync(fetchBooksAsync))
        .SelectMany(books => books.ToObservable())
        .Publish()
        .RefCount();
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

    public HashSet<Book> getBooksForTopicModeling(HashSet<string> books)
    {
        HashSet<Book> discoveredBooks = new HashSet<Book>();

        foreach (string bookName in books)
        {
            if (bookDiscovery.TryGetValue(bookName, out Book? book))
            {
                discoveredBooks.Add(book);
            }
        }

        return discoveredBooks;
    }

    public IObservable<Book> getBooksObservable () => this.bookObservable;
}
