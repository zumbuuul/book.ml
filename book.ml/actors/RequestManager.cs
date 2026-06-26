using Akka.Actor;

public class RequestManager : UntypedActor
{
    public RequestManager()
    {
        
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case "print":
                test();
                break;
        }
    }

    public void test()
    {
        Console.WriteLine("message received! " + Environment.CurrentManagedThreadId);
    }
}