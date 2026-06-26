using Akka.Actor;

public class RequestGroup : UntypedActor
{
    private String[] books;
    private BookCache kes;
    public RequestGroup(String[] b, BookCache k)
    {
        this.books = b;  
        this.kes = k; 
    }
    protected override void OnReceive(object message)
    {
        switch(message)
        {
            case "read":
            List<String> undiscovered = kes.checkCache(books);
                break;
            
        }
    }

    private void startProcessingBooks()
    {
        
    }

    private void startFetchingMissingBooks()
    {
        
    }


    public static Props Props(String[] b, BookCache k) => Akka.Actor.Props.Create(() => new RequestGroup(b, k));
}