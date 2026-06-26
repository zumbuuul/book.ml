using System.Collections.Concurrent;
using System.Reflection;

public class BookCache
{
    private BookSearch bookSearch = new BookSearch("https://www.googleapis.com/books/v1/volumes?q=");
    private ConcurrentDictionary<String, Book> bookDiscovery = new ConcurrentDictionary<string, Book>(); 

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

    public async Task<Book> fetchBook(String missingBook)
    {
        var book = await bookSearch.search(missingBook);
        bookDiscovery.AddOrUpdate(missingBook, book, (k,v) => v);
        return book;
    }
}