using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Playwright;

namespace Collector.Api;

public static partial class ApiHost
{
    private static void MapWorkflowEndpoints(WebApplication app)
    {
        app.MapPost("/api/clients/sync", async (AppDbContext db, SettingsStore settingsStore, ClientSyncService clientSync, CancellationToken ct) =>
        {
            var settings = await settingsStore.GetAsync(db, ct);
            var credentialValidation = ClientSyncService.ValidateSyncCredentials(settings.LoginUrl, settings.Login, settings.Password);
            if (credentialValidation is not null)
            {
                return ErrBadRequest(credentialValidation);
            }

            try
            {
                var result = await clientSync.SyncLiveFromRocketmanAsync(
                    db,
                    settings.LoginUrl,
                    settings.Login,
                    settings.Password,
                    ct);

                return Results.Ok(result);
            }
            catch (FileNotFoundException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_SELECTORS_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_INVALID_STATE",
                    Message = ex.Message
                });
            }
            catch (TimeoutException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "ROCKETMAN_UNAVAILABLE",
                    Message = ex.Message
                });
            }
            catch (PlaywrightException ex)
            {
                if (IsPlaywrightBrowserMissing(ex))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "SYNC_PLAYWRIGHT_NOT_INSTALLED",
                        Message = "На этом ПК не установлен Chromium для Playwright. Запустите \"Collector.exe --install-playwright\" и повторите актуализацию."
                    });
                }

                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_PLAYWRIGHT_ERROR",
                    Message = ex.Message
                });
            }
            catch (Exception ex)
            {
                return Results.Problem(
                    statusCode: 500,
                    title: "Не удалось актуализировать базу клиентов.",
                    detail: ex.Message);
            }
        });

        app.MapGet("/api/clients/sync-status", async (AppDbContext db, ClientSyncService clientSync, CancellationToken ct) =>
        {
            var status = await clientSync.GetLatestSyncStatusAsync(db, ct);
            return Results.Ok(status);
        });

        app.MapGet("/api/clients", async (HttpContext ctx, AppDbContext db, ClientSyncService clientSync, CancellationToken ct) =>
        {
            if (!TryReadOptionalMinLongQuery(ctx.Request, "snapshotId", 1, "CLIENTS_SNAPSHOT_ID_INVALID", "Параметр snapshotId должен быть положительным числом.", out var snapshotId, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadClampedIntQuery(ctx.Request, "limit", 1000, 1, 5000, "CLIENTS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "CLIENTS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await clientSync.ListClientsAsync(db, snapshotId, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/clients/{externalClientId}/debt", async (
            string externalClientId,
            AppDbContext db,
            DebtCacheService debtCache,
            CancellationToken ct) =>
        {
            var validation = DebtCacheService.ValidateExternalClientId(externalClientId);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var data = await debtCache.GetByExternalClientIdAsync(db, externalClientId, ct);
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CLIENT_NOT_FOUND",
                    Message = $"Клиент с externalClientId='{externalClientId}' не найден."
                });
            }

            return Results.Ok(data);
        });

        app.MapPost("/api/clients/{externalClientId}/debt/fetch", async (
            string externalClientId,
            AppDbContext db,
            SettingsStore settingsStore,
            DebtCacheService debtCache,
            ClientDebtFetchRequestDto payload,
            CancellationToken ct) =>
        {
            var idValidation = DebtCacheService.ValidateExternalClientId(externalClientId);
            if (idValidation is not null)
            {
                return ErrBadRequest(idValidation);
            }

            var requestValidation = DebtCacheService.ValidateFetchRequest(payload);
            if (requestValidation is not null)
            {
                return ErrBadRequest(requestValidation);
            }

            var settings = await settingsStore.GetAsync(db, ct);
            var result = await debtCache.FetchByExternalClientIdAsync(db, externalClientId, settings, payload, ct);
            if (result.Success)
            {
                return Results.Ok(result);
            }

            if (string.Equals(result.Code, "CLIENT_NOT_FOUND", StringComparison.Ordinal))
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = result.Code,
                    Message = result.Message
                });
            }

            return ErrBadRequest(new ApiErrorDto
            {
                Code = result.Code,
                Message = result.Message
            });
        });

        app.MapPost("/api/queue/preview", async (AppDbContext db, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteNotFoundAndBadRequestAsync(
                () => queue.PreviewAsync(db, payload, ct),
                "QUEUE_SNAPSHOT_NOT_FOUND",
                "QUEUE_FILTER_INVALID");
        });

        app.MapPost("/api/queue/forecast", async (AppDbContext db, SettingsStore settingsStore, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteNotFoundAndBadRequestAsync(
                async () =>
                {
                    var settings = await settingsStore.GetAsync(db, ct);
                    return await queue.ForecastAsync(db, payload, settings, ct);
                },
                "QUEUE_SNAPSHOT_NOT_FOUND",
                "QUEUE_FILTER_INVALID");
        });

        app.MapPost("/api/queue/build", async (AppDbContext db, SettingsStore settingsStore, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteQueueStateAsync(
                async () =>
                {
                    var settings = await settingsStore.GetAsync(db, ct);
                    return await queue.BuildAsync(db, payload, settings, ct);
                },
                "QUEUE_SNAPSHOT_NOT_FOUND",
                "QUEUE_FILTER_INVALID");
        });

        app.MapGet("/api/queue", async (HttpContext ctx, AppDbContext db, QueueService queue, CancellationToken ct) =>
        {
            if (!TryReadOptionalMinLongQuery(ctx.Request, "runSessionId", 1, "QUEUE_RUN_SESSION_ID_INVALID", "Параметр runSessionId должен быть положительным числом.", out var runSessionId, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadClampedIntQuery(ctx.Request, "limit", 1000, 1, 5000, "QUEUE_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "QUEUE_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            return await ExecuteNotFoundAsync(
                () => queue.ListAsync(db, runSessionId, limit, offset, ct),
                "QUEUE_RUN_SESSION_NOT_FOUND");
        });

        app.MapPost("/api/queue/{jobId:long}/preview/rebuild", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            QueueJobPreviewRequestDto? payload,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            var request = payload ?? new QueueJobPreviewRequestDto();
            return await ExecuteNotFoundAndBadRequestAsync(
                () => queue.RebuildJobPreviewAsync(db, jobId, request, ct),
                "QUEUE_JOB_NOT_FOUND",
                "QUEUE_PREVIEW_REBUILD_INVALID");
        });

        app.MapPut("/api/queue/{jobId:long}/message-override", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            QueueJobMessageOverrideRequestDto payload,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            if (payload is null)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_MESSAGE_OVERRIDE_REQUIRED",
                    Message = "Тело запроса с текстом сообщения обязательно."
                });
            }

            return await ExecuteNotFoundAndBadRequestAsync(
                () => queue.SetJobMessageOverrideAsync(db, jobId, payload, ct),
                "QUEUE_JOB_NOT_FOUND",
                "QUEUE_MESSAGE_OVERRIDE_INVALID");
        });

        app.MapDelete("/api/queue/{jobId:long}/message-override", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            return await ExecuteNotFoundAndBadRequestAsync(
                () => queue.ClearJobMessageOverrideAsync(db, jobId, ct),
                "QUEUE_JOB_NOT_FOUND",
                "QUEUE_MESSAGE_OVERRIDE_INVALID");
        });

        app.MapPost("/api/queue/jobs/remove", async (AppDbContext db, QueueService queue, QueueRemoveJobsRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRemoveJobsRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteNotFoundAndConflictAsync(
                () => queue.RemoveJobsFromPlanAsync(db, payload, ct),
                "QUEUE_RUN_SESSION_NOT_FOUND",
                "QUEUE_REMOVE_INVALID_STATE");
        });

        app.MapPost("/api/queue/retry-errors", async (AppDbContext db, QueueService queue, QueueRetryErrorsRequestDto? payload, CancellationToken ct) =>
        {
            var request = payload ?? new QueueRetryErrorsRequestDto();
            var validation = QueueService.ValidateRetryErrorsRequest(request);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteNotFoundAndConflictAsync(
                () => queue.RetryErrorsAsync(db, request, ct),
                "QUEUE_RUN_SESSION_NOT_FOUND",
                "QUEUE_RETRY_ERRORS_INVALID_STATE");
        });

        app.MapPost("/api/queue/bulk/set-template", async (AppDbContext db, QueueService queue, QueueBulkSetTemplateRequest payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateBulkSetTemplateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteNotFoundAndBadRequestAsync(
                () => queue.BulkSetTemplateAsync(db, payload, ct),
                "QUEUE_BULK_TEMPLATE_NOT_FOUND",
                "QUEUE_BULK_TEMPLATE_INVALID");
        });

        app.MapGet("/api/run/status", async (HttpContext ctx, AppDbContext db, RunService run, CancellationToken ct) =>
        {
            if (!TryReadOptionalMinLongQuery(ctx.Request, "runSessionId", 1, "RUN_SESSION_ID_INVALID", "Параметр runSessionId должен быть положительным числом.", out var runSessionId, out var parseError))
            {
                return parseError!;
            }

            return await ExecuteRunStateAsync(() => run.GetStatusAsync(db, runSessionId, ct));
        });

        app.MapGet("/api/run/history", async (HttpContext ctx, AppDbContext db, RunService run, CancellationToken ct) =>
        {
            if (!TryReadClampedIntQuery(ctx.Request, "limit", 50, 1, 500, "RUN_HISTORY_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "RUN_HISTORY_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await run.ListHistoryAsync(db, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapDelete("/api/run/history", async (AppDbContext db, RunService run, CancellationToken ct) =>
        {
            var data = await run.ClearHistoryAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPost("/api/run/start", async (AppDbContext db, RunService run, RunStartRequestDto? payload, CancellationToken ct) =>
        {
            var validation = RunService.ValidateStartRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteRunStateAsync(() => run.StartAsync(db, payload ?? new RunStartRequestDto(), ct));
        });

        app.MapPost("/api/run/stop", async (AppDbContext db, RunService run, RunStopRequestDto? payload, CancellationToken ct) =>
        {
            var validation = RunService.ValidateStopRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            return await ExecuteRunStateAsync(() => run.StopAsync(db, payload ?? new RunStopRequestDto(), ct));
        });

        app.MapPost("/api/app/shutdown", async (AppDbContext db, RunService run, IHostApplicationLifetime lifetime, CancellationToken ct) =>
        {
            try
            {
                var status = await run.GetStatusAsync(db, null, ct);
                if (status.HasRunningSession && status.RunningSessionId > 0)
                {
                    try
                    {
                        await run.StopAsync(db, new RunStopRequestDto
                        {
                            RunSessionId = status.RunningSessionId,
                            Reason = "Остановлено при завершении приложения."
                        }, ct);
                    }
                    catch
                    {
                    }
                }
            }
            catch
            {
            }

            _ = Task.Run(async () =>
            {
                await Task.Delay(300);
                lifetime.StopApplication();
            });

            return Results.Ok(new
            {
                action = "shutdown",
                message = "Приложение завершает работу."
            });
        });
    }
}
