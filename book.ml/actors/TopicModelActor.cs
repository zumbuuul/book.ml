using System.Reactive.Linq;
using Akka.Actor;

public sealed record RunTopicModeling(IObservable<HashSet<Book>> BooksStream);

public class TopicModelActor : UntypedActor
{
    public TopicModelActor()
    {
        
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case RunTopicModeling run:
                run.BooksStream.Subscribe(RunTopicModelingForBooks);
                break;
            default:
                Unhandled(message);
                break;
        }
    }

    private void RunTopicModelingForBooks(HashSet<Book> books)
    {
        Console.WriteLine("TopicModelActor received " + books.Count + " books for topic modeling");

        foreach (Book book in books)
        {
            Console.WriteLine("topic modeling input: " + book.Name);
        }
    }
}
