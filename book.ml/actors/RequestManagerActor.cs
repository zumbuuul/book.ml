using Akka.Actor;

public sealed record ProcessBookRequest(HashSet<string> Books);

public class RequestManagerActor : UntypedActor
{
    private readonly BookCache kes;
    private readonly Dictionary<Guid, IActorRef> groupsByRequestId = new();
    private readonly Dictionary<IActorRef, Guid> requestIdsByGroup = new();

    public RequestManagerActor(BookCache k)
    {
        this.kes = k;
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case ProcessBookRequest request:
                StartRequestGroup(request.Books, Sender);
                break;
            case Terminated terminated:
                RemoveRequestGroup(terminated.ActorRef);
                break;
        }
    }

    private void StartRequestGroup(HashSet<string> books, IActorRef replyTo)
    {
        Guid requestId = Guid.NewGuid();
        string actorName = $"request-group-{requestId}";
        IActorRef group = Context.ActorOf(RequestGroupActor.Props(books, kes), actorName);

        groupsByRequestId[requestId] = group;
        requestIdsByGroup[group] = requestId;
        Context.Watch(group);

        Console.WriteLine($"RequestManager started group {requestId} for {books.Count} books");
        Console.WriteLine($"Active request groups: {groupsByRequestId.Count}");

        group.Tell("read", replyTo);
    }

    private void RemoveRequestGroup(IActorRef group)
    {
        if (!requestIdsByGroup.TryGetValue(group, out Guid requestId))
        {
            return;
        }

        requestIdsByGroup.Remove(group);
        groupsByRequestId.Remove(requestId);

        Console.WriteLine($"RequestManager removed group {requestId}");
        Console.WriteLine($"Active request groups: {groupsByRequestId.Count}");
    }

    public static Props Props(BookCache k) => Akka.Actor.Props.Create(() => new RequestManagerActor(k));
}
