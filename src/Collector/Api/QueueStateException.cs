namespace Collector.Api;

public sealed class QueueStateException : Exception
{
    public QueueStateException(string code, string message, int httpStatusCode)
        : base(message)
    {
        Code = code;
        HttpStatusCode = httpStatusCode;
    }

    public string Code { get; }
    public int HttpStatusCode { get; }
}
