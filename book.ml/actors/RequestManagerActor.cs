using Akka.Actor;
using Akka.Event;

public sealed class ProcessBookRequest
{
    public ProcessBookRequest(HashSet<string> Books)
    {
        this.Books = Books;
    }

    public HashSet<string> Books { get; }
}

public class RequestManagerActor : UntypedActor
{
    private ILoggingAdapter Log {get; } = Context.GetLogger();
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
        HashSet<string> normalizedBooks = books
            .Select(book => book.Trim())
            .Where(book => !string.IsNullOrWhiteSpace(book))
            .ToHashSet();

        IActorRef group = Context.ActorOf(RequestGroupActor.Props(normalizedBooks, kes), actorName);

        groupsByRequestId[requestId] = group;
        requestIdsByGroup[group] = requestId;
        Context.Watch(group);

        Log.Info($"RequestManager started group {requestId} for {normalizedBooks.Count} books");
        Log.Info($"Active request groups: {groupsByRequestId.Count}");

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

        Log.Info($"RequestManager removed group {requestId}");
        Log.Info($"Active request groups: {groupsByRequestId.Count}");
    }

    public static Props Props(BookCache k) => Akka.Actor.Props.Create(() => new RequestManagerActor(k));
}
