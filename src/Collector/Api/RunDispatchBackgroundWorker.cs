using Collector.Data;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Collector.Api;

public sealed class RunDispatchBackgroundWorker(
    IServiceScopeFactory scopeFactory,
    ILogger<RunDispatchBackgroundWorker> logger) : BackgroundService
{
    private readonly SemaphoreSlim _tickLock = new(1, 1);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            if (!await _tickLock.WaitAsync(0, stoppingToken))
            {
                continue;
            }

            try
            {
                using var scope = scopeFactory.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                var dispatch = scope.ServiceProvider.GetRequiredService<RunDispatchService>();
                await dispatch.ProcessTickAsync(db, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Ошибка фонового диспетчера очереди.");
            }
            finally
            {
                _tickLock.Release();
            }
        }
    }
}
