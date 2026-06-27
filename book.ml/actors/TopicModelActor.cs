using System.Reactive.Linq;
using Akka.Actor;
using Microsoft.ML;
using Microsoft.ML.Transforms.Text;

public sealed class runTopicModeling
{
    public runTopicModeling(IObservable<HashSet<Book>> booksStream)
    {
        this.booksStream = booksStream;
    }

    public IObservable<HashSet<Book>> booksStream { get; }
}

public sealed class topicScore
{
    public topicScore(string topic, double percentage)
    {
        this.topic = topic;
        this.percentage = percentage;
    }

    public string topic { get; }
    public double percentage { get; }
}

public sealed class bookTopicResult
{
    public bookTopicResult(string bookName, List<topicScore> topics)
    {
        this.bookName = bookName;
        this.topics = topics;
    }

    public string bookName { get; }
    public List<topicScore> topics { get; }
}

public sealed class topicModelingCompleted
{
    public topicModelingCompleted(List<bookTopicResult> results)
    {
        this.results = results;
    }

    public List<bookTopicResult> results { get; }
}

public sealed class topicModelingFailed
{
    public topicModelingFailed(string error)
    {
        this.error = error;
    }

    public string error { get; }
}

public class TopicModelActor : UntypedActor
{
    public sealed class booksReady
    {
        public booksReady(HashSet<Book> books)
        {
            this.books = books;
        }

        public HashSet<Book> books { get; }
    }

    private IActorRef replyTo = ActorRefs.Nobody;
    private IDisposable? subscription;

    public TopicModelActor()
    {
        
    }

    protected override void OnReceive(object message)
    {
        switch (message)
        {
            case runTopicModeling run:
                replyTo = Sender;
                subscription = run.booksStream.Subscribe(
                    books => Self.Tell(new booksReady(books)),
                    error => Self.Tell(new topicModelingFailed(error.Message)));
                break;
            case booksReady ready:
                tryCompleteTopicModeling(ready.books);
                Context.Stop(Self);
                break;
            case topicModelingFailed failed:
                replyTo.Tell(failed, Self);
                Context.Stop(Self);
                break;
            default:
                Unhandled(message);
                break;
        }
    }

    private static topicModelingCompleted runTopicModeling(HashSet<Book> books)
    {
        List<topicInput> input = books
            .Where(book => !string.IsNullOrWhiteSpace(book.Description))
            .Select(book => new topicInput { bookName = book.Name, text = book.Description })
            .ToList();

        if (input.Count == 0)
        {
            return new topicModelingCompleted([]);
        }

        MLContext mlContext = new MLContext(1);
        const int topicCount = 3;
        var data = mlContext.Data.LoadFromEnumerable(input);

        var pipeline = mlContext.Transforms.Text.NormalizeText("normalizedText", "text")
            .Append(mlContext.Transforms.Text.TokenizeIntoWords("tokens", "normalizedText"))
            .Append(mlContext.Transforms.Text.RemoveDefaultStopWords("tokensWithoutStopWords", "tokens"))
            .Append(mlContext.Transforms.Conversion.MapValueToKey("tokensAsKeys", "tokensWithoutStopWords"))
            .Append(mlContext.Transforms.Text.ProduceNgrams("ngrams", "tokensAsKeys"))
            .Append(mlContext.Transforms.Text.LatentDirichletAllocation(
                "features",
                "ngrams",
                numberOfTopics: topicCount,
                maximumNumberOfIterations: 30,
                likelihoodInterval: 1000,
                numberOfThreads: 1,
                numberOfSummaryTermsPerTopic: 5));

        var model = pipeline.Fit(data);
        List<string> topicNames = getTopicNames(model.LastTransformer, topicCount);
        var transformed = model.Transform(data);
        List<topicPrediction> predictions = mlContext.Data
            .CreateEnumerable<topicPrediction>(transformed, reuseRowObject: false)
            .ToList();

        List<bookTopicResult> results = predictions
            .Select(prediction => new bookTopicResult(
                prediction.bookName,
                prediction.features.Select((score, index) =>
                    new topicScore(topicNames[index], toPercentage(score, prediction.features))).ToList()))
            .ToList();

        return new topicModelingCompleted(results);
    }

    private static List<string> getTopicNames(LatentDirichletAllocationTransformer transformer, int topicCount)
    {
        var ldaDetails = transformer.GetLdaDetails(0);

        List<string> topicNames = ldaDetails.WordScoresPerTopic
            .Take(topicCount)
            .Select((topicWords, index) =>
            {
                List<string> words = topicWords
                    .OrderByDescending(word => word.Score)
                    .Take(5)
                    .Select(word => word.Word)
                    .Where(word => !string.IsNullOrWhiteSpace(word))
                    .ToList();

                return words.Count == 0 ? "topic " + (index + 1) : string.Join(", ", words);
            })
            .ToList();

        while (topicNames.Count < topicCount)
        {
            topicNames.Add("topic " + (topicNames.Count + 1));
        }

        return topicNames;
    }

    private void tryCompleteTopicModeling(HashSet<Book> books)
    {
        try
        {
            replyTo.Tell(runTopicModeling(books), Self);
        }
        catch (Exception error)
        {
            replyTo.Tell(new topicModelingFailed(error.Message), Self);
        }
    }

    private static double toPercentage(float score, float[] scores)
    {
        float total = scores.Sum();
        return total == 0 ? 0 : Math.Round(score * 100 / total, 2);
    }

    protected override void PostStop()
    {
        subscription?.Dispose();
    }

    private sealed class topicInput
    {
        public string bookName { get; set; } = "";
        public string text { get; set; } = "";
    }

    private sealed class topicPrediction
    {
        public string bookName { get; set; } = "";
        public float[] features { get; set; } = [];
    }
}
