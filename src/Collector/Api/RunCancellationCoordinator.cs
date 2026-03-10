namespace Collector.Api;

public sealed class RunCancellationCoordinator
{
    private readonly object _sync = new();
    private readonly Dictionary<long, CancellationTokenSource> _sessionCancellation = new();

    public CancellationTokenSource CreateAttemptScope(long runSessionId, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            var sessionCts = GetOrCreateSessionSource(runSessionId);
            return CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, sessionCts.Token);
        }
    }

    public void ResetSession(long runSessionId)
    {
        if (runSessionId <= 0)
        {
            return;
        }

        lock (_sync)
        {
            if (_sessionCancellation.Remove(runSessionId, out var existing))
            {
                existing.Dispose();
            }

            _sessionCancellation[runSessionId] = new CancellationTokenSource();
        }
    }

    public void CancelSession(long runSessionId)
    {
        if (runSessionId <= 0)
        {
            return;
        }

        lock (_sync)
        {
            var sessionCts = GetOrCreateSessionSource(runSessionId);
            if (!sessionCts.IsCancellationRequested)
            {
                sessionCts.Cancel();
            }
        }
    }

    public void ClearSession(long runSessionId)
    {
        if (runSessionId <= 0)
        {
            return;
        }

        lock (_sync)
        {
            if (_sessionCancellation.Remove(runSessionId, out var existing))
            {
                existing.Dispose();
            }
        }
    }

    public bool IsSessionCancellationRequested(long runSessionId)
    {
        if (runSessionId <= 0)
        {
            return false;
        }

        lock (_sync)
        {
            return _sessionCancellation.TryGetValue(runSessionId, out var cts) && cts.IsCancellationRequested;
        }
    }

    private CancellationTokenSource GetOrCreateSessionSource(long runSessionId)
    {
        if (!_sessionCancellation.TryGetValue(runSessionId, out var sessionCts))
        {
            sessionCts = new CancellationTokenSource();
            _sessionCancellation[runSessionId] = sessionCts;
        }

        return sessionCts;
    }
}
