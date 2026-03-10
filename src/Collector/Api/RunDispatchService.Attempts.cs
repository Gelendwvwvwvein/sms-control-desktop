using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class RunDispatchService
{
    private async Task<DispatchAttemptResult> ExecuteAttemptAsync(
        AppDbContext db,
        AppSettingsDto settings,
        RunSessionRecord runSession,
        RunJobRecord job,
        SenderChannelRecord channel,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(job.Phone))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                Code = "PHONE_EMPTY",
                Detail = "У клиента отсутствует номер телефона."
            };
        }

        if (await IsPhoneInActiveStopListAsync(db, job.Phone, cancellationToken))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = JobErrorStoppedByStopList,
                Detail = "Отправка заблокирована: номер находится в стоп-листе."
            };
        }

        if (string.IsNullOrWhiteSpace(channel.Endpoint) || string.IsNullOrWhiteSpace(channel.Token))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                Code = "CHANNEL_CONFIG_INVALID",
                Detail = "У канала не задан endpoint или token."
            };
        }

        if (job.TzOffset is < -12 or > 14)
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = "TIMEZONE_OFFSET_INVALID",
                Detail = $"Некорректный timezoneOffset у клиента: {job.TzOffset}."
            };
        }

        if (!TryParseWorkWindowStrict(settings.WorkWindowStart, settings.WorkWindowEnd, out var workStart, out var workEnd))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = "WORK_WINDOW_CONFIG_INVALID",
                Detail = "Некорректно задано рабочее окно в настройках (ожидается HH:mm, end > start)."
            };
        }

        var nowUtc = DateTime.UtcNow;
        var nextAllowedUtc = AlignToWorkWindowUtc(nowUtc, job.TzOffset, workStart, workEnd);
        if (nextAllowedUtc > nowUtc.AddSeconds(1))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = true,
                CountAsAttempt = false,
                Code = "WORK_WINDOW_WAIT",
                Detail = $"Локальное время клиента вне рабочего окна. Следующая попытка: {nextAllowedUtc:O}",
                NextPlannedAtUtc = nextAllowedUtc
            };
        }

        var payloadTotal = ExtractPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw);
        var liveMode = string.Equals(runSession.Mode, RunModeLive, StringComparison.OrdinalIgnoreCase);
        var shouldFetchDebt = string.IsNullOrWhiteSpace(payloadTotal);

        if (liveMode)
        {
            var normalizedExternalClientId = NormalizeExternalClientId(job.ExternalClientId);
            if (!string.IsNullOrWhiteSpace(normalizedExternalClientId))
            {
                var cache = await db.ClientDebtCache
                    .AsNoTracking()
                    .FirstOrDefaultAsync(x => x.ExternalClientId == normalizedExternalClientId, cancellationToken);

                if (IsDebtCacheFresh(cache, nowUtc))
                {
                    shouldFetchDebt = false;
                    var cachedExactRaw = (cache?.ExactTotalRaw ?? string.Empty).Trim();
                    if (!string.IsNullOrWhiteSpace(cachedExactRaw) &&
                        !string.Equals(payloadTotal, cachedExactRaw, StringComparison.Ordinal))
                    {
                        job.PayloadJson = UpsertPayloadField(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, cachedExactRaw);
                        db.RunJobs.Update(job);
                        await db.SaveChangesAsync(cancellationToken);
                    }
                }
                else
                {
                    shouldFetchDebt = true;
                }
            }
            else
            {
                shouldFetchDebt = true;
            }
        }

        if (shouldFetchDebt)
        {
            var debtFetch = await debtCacheService.FetchByExternalClientIdAsync(
                db,
                job.ExternalClientId,
                settings,
                new ClientDebtFetchRequestDto
                {
                    TimeoutMs = 30000,
                    Headed = false
                },
                cancellationToken);

            if (!debtFetch.Success || string.IsNullOrWhiteSpace(debtFetch.Debt?.ExactTotalRaw))
            {
                return BuildDebtFetchFailureResult(debtFetch);
            }

            job.PayloadJson = UpsertPayloadField(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, debtFetch.Debt.ExactTotalRaw);
            db.RunJobs.Update(job);
            if (liveMode)
            {
                AddEvent(
                    db,
                    runSession.Id,
                    job.Id,
                    "debt_refreshed",
                    "info",
                    $"Задача #{job.Id}: сумма долга обновлена из карточки клиента.",
                    new
                    {
                        runSessionId = runSession.Id,
                        runJobId = job.Id,
                        externalClientId = job.ExternalClientId,
                        totalWithCommissionRaw = debtFetch.Debt.ExactTotalRaw
                    });
            }
            await db.SaveChangesAsync(cancellationToken);
        }

        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var messageOverride = ExtractPayloadString(job.PayloadJson, PayloadFieldMessageOverride);
        var usesMessageOverride = !string.IsNullOrWhiteSpace(messageOverride);
        var rendered = !usesMessageOverride
            ? ruleEngine.BuildDispatchMessage(activeTemplates, job, settings.DebtBufferAmount)
            : new RuleEngineMessageResult
            {
                TemplateId = job.TemplateId,
                MessageText = messageOverride,
                UsedFallback = false,
                ErrorCode = string.Empty,
                ErrorMessage = string.Empty
            };

        if (!string.IsNullOrWhiteSpace(rendered.ErrorCode))
        {
            job.PreviewStatus = PreviewStatusError;
            job.PreviewText = string.Empty;
            job.PreviewVariablesJson = "{}";
            job.PreviewUpdatedAtUtc = DateTime.UtcNow;
            job.PreviewErrorCode = rendered.ErrorCode;
            job.PreviewErrorDetail = rendered.ErrorMessage;
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = rendered.ErrorCode,
                Detail = rendered.ErrorMessage
            };
        }

        var previewTemplate = rendered.TemplateId.HasValue && rendered.TemplateId.Value > 0
            ? activeTemplates.FirstOrDefault(x => x.Id == rendered.TemplateId.Value)
            : null;
        var resolvedTemplateKind = (previewTemplate?.Kind ?? string.Empty).Trim();
        var resolvedTemplateComment = (previewTemplate?.CommentText ?? string.Empty).Trim();
        var previewPayloadTotal = ExtractPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw);
        var approxDebtText = RuleEngineService.BuildApproxDebtText(job.PayloadJson, settings.DebtBufferAmount);
        job.PreviewStatus = PreviewStatusReady;
        job.PreviewText = rendered.MessageText;
        job.PreviewVariablesJson = JsonSerializer.Serialize(new
        {
            fullFio = (job.ClientFio ?? string.Empty).Trim(),
            totalWithCommissionRaw = previewPayloadTotal,
            approxDebtText,
            templateId = rendered.TemplateId,
            templateKind = previewTemplate?.Kind ?? string.Empty,
            templateName = previewTemplate?.Name ?? string.Empty,
            messageOverride = usesMessageOverride
        });
        job.PreviewUpdatedAtUtc = DateTime.UtcNow;
        job.PreviewErrorCode = string.Empty;
        job.PreviewErrorDetail = string.Empty;

        var sendResult = await _traccarSender.SendAsync(new TraccarSmsSendRequest
        {
            Url = channel.Endpoint,
            Token = channel.Token,
            To = job.Phone,
            Message = rendered.MessageText,
            TimeoutMs = 15000
        }, cancellationToken);

        if (sendResult.Success)
        {
            return new DispatchAttemptResult
            {
                Success = true,
                IsTransient = false,
                Code = "SENT",
                Detail = sendResult.Detail,
                MessageText = rendered.MessageText,
                TemplateId = rendered.TemplateId,
                TemplateKind = resolvedTemplateKind,
                TemplateCommentText = resolvedTemplateComment,
                UsedMessageOverride = usesMessageOverride,
                StatusCode = sendResult.StatusCode,
                ResponseBody = sendResult.ResponseBody,
                Error = sendResult.Error
            };
        }

        return new DispatchAttemptResult
        {
            Success = false,
            IsTransient = IsTransientGatewayError(sendResult.StatusCode),
            Code = "GATEWAY_SEND_FAILED",
            Detail = BuildGatewayFailureDetail(sendResult),
            MessageText = rendered.MessageText,
            TemplateId = rendered.TemplateId,
            TemplateKind = resolvedTemplateKind,
            TemplateCommentText = resolvedTemplateComment,
            UsedMessageOverride = usesMessageOverride,
            StatusCode = sendResult.StatusCode,
            ResponseBody = sendResult.ResponseBody,
            Error = sendResult.Error
        };
    }

    private static bool IsChannelStateNeutralError(string? code)
    {
        return string.Equals(code, "CHANNEL_UNAVAILABLE", StringComparison.Ordinal) ||
               string.Equals(code, JobErrorStoppedByStopList, StringComparison.Ordinal) ||
               string.Equals(code, JobErrorStoppedByOperator, StringComparison.Ordinal);
    }

    private static string BuildGatewayFailureDetail(TraccarSmsSendResult sendResult)
    {
        var normalizedDetail = TruncateForLog(sendResult.Detail, 240);
        var normalizedBody = TruncateForLog(sendResult.ResponseBody, 240);
        var normalizedError = TruncateForLog(sendResult.Error, 200);
        var statusPart = sendResult.StatusCode > 0 ? $"HTTP {sendResult.StatusCode}" : string.Empty;

        var parts = new[]
        {
            normalizedDetail,
            statusPart,
            string.IsNullOrWhiteSpace(normalizedBody) ? string.Empty : $"Ответ gateway: {normalizedBody}",
            string.IsNullOrWhiteSpace(normalizedError) ? string.Empty : $"Ошибка транспорта: {normalizedError}"
        };

        var composed = string.Join(
            ". ",
            parts.Where(x => !string.IsNullOrWhiteSpace(x)));

        return string.IsNullOrWhiteSpace(composed)
            ? "Ошибка отправки через gateway."
            : composed;
    }

    private static string TruncateForLog(string? value, int maxLen)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var normalized = value
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        if (normalized.Length > maxLen)
        {
            normalized = $"{normalized[..maxLen]}...";
        }

        return normalized;
    }

    private static string BuildLogDetailSuffix(string? detail)
    {
        var raw = TruncateForLog(detail, 240);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        return $" Детали: {raw}";
    }

    private static bool IsTransientGatewayError(int statusCode)
    {
        if (statusCode <= 0) return true;
        return statusCode == 408 || statusCode == 429 || statusCode >= 500;
    }

    private static DispatchAttemptResult BuildDebtFetchFailureResult(ClientDebtFetchResultDto result)
    {
        var code = string.IsNullOrWhiteSpace(result.Code) ? "DEBT_FETCH_FAILED" : result.Code.Trim();
        var detail = string.IsNullOrWhiteSpace(result.Message)
            ? "Не удалось получить сумму долга из карточки клиента."
            : result.Message.Trim();

        var nonRetryable = string.Equals(code, "DEBT_FETCH_SETTINGS_MISSING", StringComparison.OrdinalIgnoreCase) ||
                           string.Equals(code, "DEBT_CARD_URL_MISSING", StringComparison.OrdinalIgnoreCase) ||
                           string.Equals(code, "CLIENT_NOT_FOUND", StringComparison.OrdinalIgnoreCase);

        return new DispatchAttemptResult
        {
            Success = false,
            IsTransient = !nonRetryable,
            CountAsAttempt = !nonRetryable,
            Code = code,
            Detail = detail
        };
    }

    private static string BuildChannelsSignature(IReadOnlyList<SenderChannelRecord> availableChannels)
    {
        if (availableChannels.Count == 0)
        {
            return "none";
        }

        return string.Join(
            "|",
            availableChannels
                .OrderBy(x => x.Id)
                .Select(x => $"{x.Id}:{x.Status}:{x.FailStreak}"));
    }

    private static TimeOnly ParseHm(string? raw, TimeOnly fallback)
    {
        if (TimeOnly.TryParseExact(
                (raw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var parsed))
        {
            return parsed;
        }

        return fallback;
    }

    private static bool TryParseWorkWindowStrict(
        string? startRaw,
        string? endRaw,
        out TimeOnly start,
        out TimeOnly end)
    {
        start = default;
        end = default;

        if (!TimeOnly.TryParseExact(
                (startRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out start))
        {
            return false;
        }

        if (!TimeOnly.TryParseExact(
                (endRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out end))
        {
            return false;
        }

        return end > start;
    }

    private static DateTime AlignToWorkWindowUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        return GetWindowSlotUtc(utc, tzOffsetFromMoscow, start, end);
    }

    private static DateTime GetWindowSlotUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        // timezone_offset в snapshot задается относительно Москвы.
        var clientUtcOffset = TimeSpan.FromHours(3 + tzOffsetFromMoscow);
        var local = utc + clientUtcOffset;
        var localDate = DateOnly.FromDateTime(local);
        var localTime = TimeOnly.FromDateTime(local);

        DateTime localPlanned;
        if (localTime < start)
        {
            localPlanned = localDate.ToDateTime(start);
        }
        else if (localTime >= end)
        {
            localPlanned = localDate.AddDays(1).ToDateTime(start);
        }
        else
        {
            localPlanned = local;
        }

        return DateTime.SpecifyKind(localPlanned - clientUtcOffset, DateTimeKind.Utc);
    }

    private static string ExtractPayloadString(string? payloadJson, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(payloadJson) || string.IsNullOrWhiteSpace(propertyName))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return string.Empty;
            }

            if (!doc.RootElement.TryGetProperty(propertyName, out var node))
            {
                return string.Empty;
            }

            if (node.ValueKind == JsonValueKind.String)
            {
                return (node.GetString() ?? string.Empty).Trim();
            }

            if (node.ValueKind == JsonValueKind.Number)
            {
                return node.GetRawText().Trim();
            }
        }
        catch
        {
            return string.Empty;
        }

        return string.Empty;
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

}
