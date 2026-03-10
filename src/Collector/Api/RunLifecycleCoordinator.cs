namespace Collector.Api;

public sealed class RunLifecycleCoordinator
{
    private readonly SemaphoreSlim _gate = new(1, 1);

    public Task WaitAsync(CancellationToken cancellationToken) => _gate.WaitAsync(cancellationToken);

    public void Release() => _gate.Release();
}
