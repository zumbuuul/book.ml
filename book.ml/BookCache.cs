using System.Collections.Concurrent;

public class BookCache
{
    private ConcurrentDictionary<String, Book> bookDiscovery = new ConcurrentDictionary<string, Book>(); 

    public List<String> checkCache(String[] books)
    {
        List<String> undiscoveredBooks = new List<String>();

        foreach(String book in books)
        {
            if(!bookDiscovery.ContainsKey(book))
                undiscoveredBooks.Add(book);   
            
        }  

        return undiscoveredBooks; 
    }
}