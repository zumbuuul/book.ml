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
    private readonly ConcurrentDictionary<string, byte> trackedBookNames = new(); 
    private readonly ConcurrentDictionary<string, byte> inFlightBooks = new();

    public BookCache()
    {
        Console.WriteLine("cache thread " + Environment.CurrentManagedThreadId);

        IObservable<string> periodicRefreshStream = Observable
            .Timer(TimeSpan.Zero, TimeSpan.FromSeconds(5), TaskPoolScheduler.Default)
            .SelectMany(_ =>
            {
                Console.WriteLine("Broj kljuceva " + trackedBookNames.Keys.Count);
                return trackedBookNames.Keys.ToObservable();
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

    public void requestMissingBooks(IEnumerable<string> books)
    {
        foreach (string book in books.Select(normalizeBookName).Where(book => !string.IsNullOrWhiteSpace(book)))
        {
            trackedBookNames.TryAdd(book, 0);
            missingBookRequestSink.OnNext(book);
        }
    }

    public IObservable<Book> getBooksObservable () => this.bookObservable;

    private IObservable<Book> fetchBookThroughRx(string bookName)
    {
        return Observable.FromAsync(() => bookSearch.search(bookName))
            .Where(book => !string.IsNullOrWhiteSpace(book.Description))
            .Do(book =>
            {
                string normalizedBookName = normalizeBookName(book.Name);
                trackedBookNames.TryAdd(normalizedBookName, 0);
                Console.WriteLine("Rx emitted book " + normalizedBookName + " on thread " + Environment.CurrentManagedThreadId);
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
