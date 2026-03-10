using System.Text.Json;
using System.Text.Json.Nodes;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class DebtCacheService(
    RocketmanCommentService rocketmanCommentService,
    SettingsStore settingsStore,
    QueueService queueService)
{
    public static ApiErrorDto? ValidateExternalClientId(string? externalClientId)
    {
        if (string.IsNullOrWhiteSpace(externalClientId))
        {
            return new ApiErrorDto
            {
                Code = "CLIENT_EXTERNAL_ID_REQUIRED",
                Message = "externalClientId обязателен."
            };
        }

        if (externalClientId.Trim().Length > 64)
        {
            return new ApiErrorDto
            {
                Code = "CLIENT_EXTERNAL_ID_INVALID",
                Message = "externalClientId слишком длинный (максимум 64 символа)."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateFetchRequest(ClientDebtFetchRequestDto? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto
            {
                Code = "CFG_REQUIRED_MISSING",
                Message = "Тело запроса пустое."
            };
        }

        if (payload.TimeoutMs is < 5000 or > 120000)
        {
            return new ApiErrorDto
            {
                Code = "DEBT_TIMEOUT_INVALID",
                Message = "timeoutMs должен быть в диапазоне 5000..120000."
            };
        }

        return null;
    }

    public async Task<ClientDebtStateDto?> GetByExternalClientIdAsync(
        AppDbContext db,
        string externalClientId,
        CancellationToken cancellationToken)
    {
        var normalizedExternalClientId = NormalizeExternalClientId(externalClientId);
        var row = await ResolveLatestClientRowAsync(db, normalizedExternalClientId, asNoTracking: true, cancellationToken);
        var cache = await db.ClientDebtCache
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.ExternalClientId == normalizedExternalClientId, cancellationToken);

        if (row is null && cache is null)
        {
            return null;
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        return BuildStateDto(normalizedExternalClientId, row, cache, settings.DebtBufferAmount);
    }

    public async Task<ClientDebtFetchResultDto> FetchByExternalClientIdAsync(
        AppDbContext db,
        string externalClientId,
        AppSettingsDto settings,
        ClientDebtFetchRequestDto payload,
        CancellationToken cancellationToken)
    {
        var normalizedExternalClientId = NormalizeExternalClientId(externalClientId);
        var row = await ResolveLatestClientRowAsync(db, normalizedExternalClientId, asNoTracking: false, cancellationToken);
        var cache = await db.ClientDebtCache
            .FirstOrDefaultAsync(x => x.ExternalClientId == normalizedExternalClientId, cancellationToken);

        if (row is null && cache is null)
        {
            return new ClientDebtFetchResultDto
            {
                Success = false,
                Code = "CLIENT_NOT_FOUND",
                Message = $"Клиент с externalClientId='{normalizedExternalClientId}' не найден."
            };
        }

        var cardUrl = FirstNonEmpty(row?.CardUrl, cache?.CardUrl);
        if (string.IsNullOrWhiteSpace(cardUrl))
        {
            var cachedError = await UpsertErrorAsync(
                db,
                normalizedExternalClientId,
                row,
                cache,
                code: "DEBT_CARD_URL_MISSING",
                detail: "У клиента не найден cardUrl для загрузки суммы долга.",
                debtBufferAmount: settings.DebtBufferAmount,
                cancellationToken);

            return new ClientDebtFetchResultDto
            {
                Success = false,
                Code = "DEBT_CARD_URL_MISSING",
                Message = "У клиента не найден cardUrl для загрузки суммы долга.",
                Debt = cachedError
            };
        }

        if (string.IsNullOrWhiteSpace(settings.LoginUrl) ||
            string.IsNullOrWhiteSpace(settings.Login) ||
            string.IsNullOrWhiteSpace(settings.Password))
        {
            var cachedError = await UpsertErrorAsync(
                db,
                normalizedExternalClientId,
                row,
                cache,
                code: "DEBT_FETCH_SETTINGS_MISSING",
                detail: "Для загрузки суммы долга заполните Login URL, логин и пароль в настройках.",
                debtBufferAmount: settings.DebtBufferAmount,
                cancellationToken);

            return new ClientDebtFetchResultDto
            {
                Success = false,
                Code = "DEBT_FETCH_SETTINGS_MISSING",
                Message = "Для загрузки суммы долга заполните Login URL, логин и пароль в настройках.",
                Debt = cachedError
            };
        }

        var fetchResult = await rocketmanCommentService.FetchTotalWithCommissionAsync(
            settings,
            new RocketmanCardTotalRequest
            {
                CardUrl = cardUrl,
                TimeoutMs = payload.TimeoutMs,
                Headed = payload.Headed
            },
            cancellationToken);

        if (!fetchResult.Success || string.IsNullOrWhiteSpace(fetchResult.TotalWithCommissionRaw))
        {
            var code = string.IsNullOrWhiteSpace(fetchResult.Code) ? "DEBT_FETCH_FAILED" : fetchResult.Code;
            var message = string.IsNullOrWhiteSpace(fetchResult.Message)
                ? "Не удалось получить сумму долга из карточки клиента."
                : fetchResult.Message;

            var cachedError = await UpsertErrorAsync(
                db,
                normalizedExternalClientId,
                row,
                cache,
                code,
                detail: message,
                debtBufferAmount: settings.DebtBufferAmount,
                cancellationToken);

            return new ClientDebtFetchResultDto
            {
                Success = false,
                Code = code,
                Message = message,
                Debt = cachedError
            };
        }

        var nowUtc = DateTime.UtcNow;
        var exactRaw = NormalizeDebtText(fetchResult.TotalWithCommissionRaw);
        var approxText = BuildApproxText(exactRaw, settings.DebtBufferAmount);
        var approxValue = ParseApproxValue(approxText);

        var target = cache ?? new ClientDebtCacheRecord
        {
            ExternalClientId = normalizedExternalClientId,
            Phone = NormalizePhone(row?.Phone),
            CardUrl = cardUrl,
            ExactTotalRaw = exactRaw,
            ApproxTotalText = approxText,
            ApproxTotalValue = approxValue,
            Status = DebtStatuses.Ready,
            Source = DebtSources.LiveFetch,
            LastFetchedAtUtc = nowUtc,
            CreatedAtUtc = nowUtc,
            UpdatedAtUtc = nowUtc
        };

        target.Phone = NormalizePhone(FirstNonEmpty(row?.Phone, target.Phone));
        target.CardUrl = FirstNonEmpty(cardUrl, target.CardUrl);
        target.ExactTotalRaw = exactRaw;
        target.ApproxTotalText = approxText;
        target.ApproxTotalValue = approxValue;
        target.Status = DebtStatuses.Ready;
        target.Source = DebtSources.LiveFetch;
        target.LastFetchedAtUtc = nowUtc;
        target.UpdatedAtUtc = nowUtc;
        target.LastErrorCode = null;
        target.LastErrorDetail = null;

        if (cache is null)
        {
            db.ClientDebtCache.Add(target);
        }
        else
        {
            db.ClientDebtCache.Update(target);
        }

        if (row is not null)
        {
            row.TotalWithCommissionRaw = exactRaw;
            row.CardUrl = FirstNonEmpty(row.CardUrl, cardUrl);
            db.ClientSnapshotRows.Update(row);
        }

        await UpdatePendingRunJobsPayloadTotalAsync(db, normalizedExternalClientId, exactRaw, cancellationToken);
        await queueService.RebuildPersistedPreviewsForExternalClientIdAsync(
            db,
            normalizedExternalClientId,
            settings,
            cancellationToken,
            saveChanges: false);
        await db.SaveChangesAsync(cancellationToken);

        return new ClientDebtFetchResultDto
        {
            Success = true,
            Code = "DEBT_FETCH_OK",
            Message = "Сумма долга успешно загружена из карточки клиента.",
            Debt = BuildStateDto(normalizedExternalClientId, row, target, settings.DebtBufferAmount)
        };
    }

    private static async Task<ClientSnapshotRow?> ResolveLatestClientRowAsync(
        AppDbContext db,
        string externalClientId,
        bool asNoTracking,
        CancellationToken cancellationToken)
    {
        IQueryable<ClientSnapshotRow> query = db.ClientSnapshotRows
            .Where(x => x.ExternalClientId == externalClientId);

        if (asNoTracking)
        {
            query = query.AsNoTracking();
        }

        return await query
            .OrderByDescending(x => x.SnapshotId)
            .ThenByDescending(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);
    }

    private async Task<ClientDebtStateDto> UpsertErrorAsync(
        AppDbContext db,
        string externalClientId,
        ClientSnapshotRow? row,
        ClientDebtCacheRecord? cache,
        string code,
        string detail,
        int debtBufferAmount,
        CancellationToken cancellationToken)
    {
        var nowUtc = DateTime.UtcNow;
        var target = cache ?? new ClientDebtCacheRecord
        {
            ExternalClientId = externalClientId,
            Phone = NormalizePhone(row?.Phone),
            CardUrl = FirstNonEmpty(row?.CardUrl, string.Empty),
            ExactTotalRaw = string.Empty,
            ApproxTotalText = string.Empty,
            Status = DebtStatuses.Error,
            Source = DebtSources.LiveFetch,
            CreatedAtUtc = nowUtc,
            UpdatedAtUtc = nowUtc
        };

        target.Phone = NormalizePhone(FirstNonEmpty(row?.Phone, target.Phone));
        target.CardUrl = FirstNonEmpty(row?.CardUrl, target.CardUrl);
        target.Status = DebtStatuses.Error;
        target.Source = DebtSources.LiveFetch;
        target.UpdatedAtUtc = nowUtc;
        target.LastErrorCode = code;
        target.LastErrorDetail = detail;

        if (cache is null)
        {
            db.ClientDebtCache.Add(target);
        }
        else
        {
            db.ClientDebtCache.Update(target);
        }

        await db.SaveChangesAsync(cancellationToken);
        return BuildStateDto(externalClientId, row, target, debtBufferAmount);
    }

    private static async Task UpdatePendingRunJobsPayloadTotalAsync(
        AppDbContext db,
        string externalClientId,
        string exactRaw,
        CancellationToken cancellationToken)
    {
        var jobs = await db.RunJobs
            .Where(x => x.ExternalClientId == externalClientId)
            .Where(x =>
                x.Status == RunJobStatuses.Queued ||
                x.Status == RunJobStatuses.Retry ||
                x.Status == RunJobStatuses.Running ||
                x.Status == RunJobStatuses.Stopped)
            .ToListAsync(cancellationToken);

        if (jobs.Count == 0)
        {
            return;
        }

        foreach (var job in jobs)
        {
            job.PayloadJson = UpsertPayloadField(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, exactRaw);
        }

        db.RunJobs.UpdateRange(jobs);
    }

    private static string UpsertPayloadField(string? payloadJson, string propertyName, string value)
    {
        JsonObject root;
        try
        {
            root = JsonNode.Parse(payloadJson ?? string.Empty) as JsonObject ?? new JsonObject();
        }
        catch
        {
            root = new JsonObject();
        }

        root[propertyName] = value;
        return root.ToJsonString();
    }

    private static ClientDebtStateDto BuildStateDto(
        string externalClientId,
        ClientSnapshotRow? row,
        ClientDebtCacheRecord? cache,
        int debtBufferAmount)
    {
        var exactRaw = FirstNonEmpty(cache?.ExactTotalRaw, row?.TotalWithCommissionRaw);
        var approxText = !string.IsNullOrWhiteSpace(exactRaw)
            ? BuildApproxText(exactRaw, debtBufferAmount)
            : FirstNonEmpty(cache?.ApproxTotalText, string.Empty);
        var approxValue = ParseApproxValue(approxText);

        var status = FirstNonEmpty(cache?.Status, string.IsNullOrWhiteSpace(exactRaw) ? DebtStatuses.Empty : DebtStatuses.Ready);
        var source = FirstNonEmpty(cache?.Source, string.IsNullOrWhiteSpace(exactRaw) ? string.Empty : DebtSources.Snapshot);

        return new ClientDebtStateDto
        {
            FoundClient = row is not null || cache is not null,
            ExternalClientId = externalClientId,
            Phone = NormalizePhone(FirstNonEmpty(row?.Phone, cache?.Phone)),
            CardUrl = FirstNonEmpty(row?.CardUrl, cache?.CardUrl),
            ExactTotalRaw = exactRaw,
            ApproxTotalText = approxText,
            ApproxTotalValue = approxValue,
            Status = status,
            Source = source,
            LastFetchedAtUtc = cache?.LastFetchedAtUtc,
            UpdatedAtUtc = cache?.UpdatedAtUtc,
            LastErrorCode = cache?.LastErrorCode ?? string.Empty,
            LastErrorDetail = cache?.LastErrorDetail ?? string.Empty
        };
    }

    private static string BuildApproxText(string exactRaw, int debtBufferAmount)
    {
        if (string.IsNullOrWhiteSpace(exactRaw))
        {
            return string.Empty;
        }

        var payload = JsonSerializer.Serialize(new
        {
            totalWithCommissionRaw = exactRaw
        });
        return RuleEngineService.BuildApproxDebtText(payload, debtBufferAmount);
    }

    private static int? ParseApproxValue(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var digits = new string(value.Where(char.IsDigit).ToArray());
        if (string.IsNullOrWhiteSpace(digits))
        {
            return null;
        }

        return int.TryParse(digits, out var parsed) ? parsed : null;
    }

    private static string NormalizeExternalClientId(string externalClientId)
    {
        return (externalClientId ?? string.Empty).Trim();
    }

    private static string NormalizePhone(string? rawPhone)
    {
        return PhoneNormalizer.Normalize(rawPhone, coerceRussianLocalNumbers: true);
    }

    private static string NormalizeDebtText(string? raw)
    {
        return (raw ?? string.Empty)
            .Replace('\u00A0', ' ')
            .Trim();
    }

    private static string FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value.Trim();
            }
        }

        return string.Empty;
    }
}
