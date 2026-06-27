using System.Collections.Concurrent;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;

public class BookCache
{
    private readonly IObservable<Book> bookObservable;
    private readonly ISubject<string> missingBookRequestSink = Subject.Synchronize(new Subject<string>());
    private readonly IDisposable streamConnection;

    private readonly BookSearch bookSearch = new BookSearch("https://www.googleapis.com/books/v1/volumes?q=");
    private readonly ConcurrentDictionary<string, Book> bookDiscovery = new(); 
    private readonly ConcurrentDictionary<string, byte> inFlightBooks = new();

    public BookCache()
    {
        Console.WriteLine("cache thread " + Environment.CurrentManagedThreadId);

        IObservable<string> periodicRefreshStream = Observable
            .Timer(TimeSpan.Zero, TimeSpan.FromSeconds(5), TaskPoolScheduler.Default)
            .SelectMany(_ =>
            {
                Console.WriteLine("Broj kljuceva " + bookDiscovery.Keys.Count);
                return bookDiscovery.Keys.ToObservable();
            });

        IObservable<string> requestedBooksStream = missingBookRequestSink
            .ObserveOn(TaskPoolScheduler.Default);

        IConnectableObservable<Book> connectedObservable = periodicRefreshStream
            .Merge(requestedBooksStream)
            .Select(normalizeBookName)
            .Where(bookName => !string.IsNullOrWhiteSpace(bookName))
            .Where(bookName => inFlightBooks.TryAdd(bookName, 0))
            .SelectMany(fetchBookThroughRx)
            .Publish();

        bookObservable = connectedObservable;
        streamConnection = connectedObservable.Connect();
    }

    public HashSet<String> checkCache(HashSet<String> books)
    {
        HashSet<String> undiscoveredBooks = new HashSet<String>();

        foreach(String book in books)
        {
            string normalizedBookName = normalizeBookName(book);
            if(!bookDiscovery.ContainsKey(normalizedBookName))
                undiscoveredBooks.Add(normalizedBookName); 
                
            
        }  

        return undiscoveredBooks; 
    }

    public void requestMissingBooks(IEnumerable<string> books)
    {
        foreach (string book in books.Select(normalizeBookName).Where(book => !string.IsNullOrWhiteSpace(book)))
        {
            missingBookRequestSink.OnNext(book);
        }
    }

    public HashSet<Book> getBooksForTopicModeling(HashSet<string> books)
    {
        HashSet<Book> discoveredBooks = new HashSet<Book>();

        foreach (string bookName in books)
        {
            string normalizedBookName = normalizeBookName(bookName);
            if (bookDiscovery.TryGetValue(normalizedBookName, out Book? book))
            {
                discoveredBooks.Add(book);
            }
        }

        return discoveredBooks;
    }

    public IObservable<Book> getBooksObservable () => this.bookObservable;

    private IObservable<Book> fetchBookThroughRx(string bookName)
    {
        return Observable.FromAsync(() => bookSearch.search(bookName))
            .Where(book => !string.IsNullOrWhiteSpace(book.Description))
            .Do(book =>
            {
                string normalizedBookName = normalizeBookName(book.Name);
                bookDiscovery.AddOrUpdate(normalizedBookName, book, (_, _) => book);
                Console.WriteLine("Rx stored book " + normalizedBookName + " on thread " + Environment.CurrentManagedThreadId);
            })
            .Catch<Book, Exception>(error =>
            {
                Console.WriteLine("Rx failed while fetching " + bookName + " - " + error.Message);
                return Observable.Empty<Book>();
            })
            .Finally(() => inFlightBooks.TryRemove(bookName, out _));
    }

    private static string normalizeBookName(string bookName) => bookName.Trim();
}
