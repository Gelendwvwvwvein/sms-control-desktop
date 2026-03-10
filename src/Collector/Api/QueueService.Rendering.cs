using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class QueueService
{
    private static QueueJobDto MapJobToDto(
        RunJobRecord row,
        IReadOnlyDictionary<long, TemplateRecord> templatesById,
        IReadOnlyDictionary<long, SenderChannelRecord> channelsById,
        ClientDebtCacheRecord? debtCache,
        IReadOnlyDictionary<string, int> dialogCountsByPhone,
        int debtBufferAmount)
    {
        TemplateRecord? template = null;
        if (row.TemplateId.HasValue && row.TemplateId.Value > 0)
        {
            templatesById.TryGetValue(row.TemplateId.Value, out template);
        }

        SenderChannelRecord? channel = null;
        if (row.ChannelId.HasValue && row.ChannelId.Value > 0)
        {
            channelsById.TryGetValue(row.ChannelId.Value, out channel);
        }

        var normalizedPhone = NormalizePhone(row.Phone);
        var dialogCount = !string.IsNullOrWhiteSpace(normalizedPhone) &&
                          dialogCountsByPhone.TryGetValue(normalizedPhone, out var foundCount)
            ? foundCount
            : 0;

        var exactTotalRaw = FirstNonEmpty(debtCache?.ExactTotalRaw, TryExtractPayloadTotal(row.PayloadJson));
        var approxDebtText = !string.IsNullOrWhiteSpace(exactTotalRaw)
            ? BuildApproxDebtText(exactTotalRaw, debtBufferAmount)
            : FirstNonEmpty(debtCache?.ApproxTotalText, string.Empty);
        var approxDebtValue = ParseApproxValue(approxDebtText);
        var messageOverrideText = TryExtractPayloadString(row.PayloadJson, PayloadFieldMessageOverride);

        return new QueueJobDto
        {
            Id = row.Id,
            RunSessionId = row.RunSessionId,
            ExternalClientId = row.ExternalClientId,
            ClientFio = row.ClientFio,
            Phone = row.Phone,
            TzOffset = row.TzOffset,
            DaysOverdue = row.DaysOverdue,
            TemplateId = row.TemplateId,
            TemplateName = template?.Name ?? string.Empty,
            TemplateKind = template?.Kind ?? string.Empty,
            TemplateStatus = template?.Status ?? string.Empty,
            ChannelId = row.ChannelId,
            ChannelName = channel?.Name ?? string.Empty,
            ChannelStatus = channel?.Status ?? string.Empty,
            DeliveryType = row.DeliveryType,
            Status = row.Status,
            Attempts = row.Attempts,
            MaxAttempts = row.MaxAttempts,
            PlannedAtUtc = row.PlannedAtUtc,
            SentAtUtc = row.SentAtUtc,
            LastErrorCode = row.LastErrorCode ?? string.Empty,
            LastErrorDetail = row.LastErrorDetail ?? string.Empty,
            CardUrl = TryExtractCardUrl(row.PayloadJson),
            TotalWithCommissionRaw = exactTotalRaw,
            DebtApproxText = approxDebtText,
            DebtApproxValue = approxDebtValue,
            DebtStatus = FirstNonEmpty(debtCache?.Status, string.IsNullOrWhiteSpace(exactTotalRaw) ? DebtStatuses.Empty : DebtStatuses.Ready),
            DebtSource = FirstNonEmpty(debtCache?.Source, string.IsNullOrWhiteSpace(exactTotalRaw) ? string.Empty : "payload"),
            DebtUpdatedAtUtc = debtCache?.UpdatedAtUtc,
            DebtErrorCode = debtCache?.LastErrorCode ?? string.Empty,
            DebtErrorDetail = debtCache?.LastErrorDetail ?? string.Empty,
            PreviewStatus = FirstNonEmpty(row.PreviewStatus, string.IsNullOrWhiteSpace(row.PreviewText) ? PreviewStatusEmpty : PreviewStatusReady),
            PreviewText = row.PreviewText ?? string.Empty,
            PreviewVariablesJson = row.PreviewVariablesJson ?? string.Empty,
            PreviewUpdatedAtUtc = row.PreviewUpdatedAtUtc,
            PreviewErrorCode = row.PreviewErrorCode ?? string.Empty,
            PreviewErrorDetail = row.PreviewErrorDetail ?? string.Empty,
            HasMessageOverride = !string.IsNullOrWhiteSpace(messageOverrideText),
            MessageOverrideText = messageOverrideText,
            DialogStatus = dialogCount > 0 ? "has_history" : "none",
            DialogMessagesCount = dialogCount
        };
    }

    private static string TryExtractCardUrl(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty(PayloadFields.CardUrl, out var cardUrlEl))
            {
                return string.Empty;
            }

            var value = cardUrlEl.GetString() ?? string.Empty;
            return value.Trim();
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string TryExtractPayloadTotal(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty(PayloadFields.TotalWithCommissionRaw, out var totalEl))
            {
                return string.Empty;
            }

            return totalEl.ValueKind switch
            {
                JsonValueKind.String => (totalEl.GetString() ?? string.Empty).Trim(),
                JsonValueKind.Number => totalEl.GetRawText().Trim(),
                _ => string.Empty
            };
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string TryExtractPayloadString(string? payloadJson, string field)
    {
        if (string.IsNullOrWhiteSpace(payloadJson) || string.IsNullOrWhiteSpace(field))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty(field, out var node))
            {
                return string.Empty;
            }

            return node.ValueKind switch
            {
                JsonValueKind.String => (node.GetString() ?? string.Empty).Trim(),
                JsonValueKind.Number => node.GetRawText().Trim(),
                _ => string.Empty
            };
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string BuildJobPayloadJson(string? cardUrl, string? totalWithCommissionRaw)
    {
        return JsonSerializer.Serialize(new
        {
            cardUrl = (cardUrl ?? string.Empty).Trim(),
            totalWithCommissionRaw = (totalWithCommissionRaw ?? string.Empty).Trim()
        });
    }

    private static string UpsertPayloadString(string? payloadJson, string field, string value)
    {
        JsonObject root;
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            root = new JsonObject();
        }
        else
        {
            try
            {
                root = JsonNode.Parse(payloadJson) as JsonObject ?? new JsonObject();
            }
            catch
            {
                root = new JsonObject();
            }
        }

        root[field] = value;
        return root.ToJsonString();
    }

    private static string RemovePayloadString(string? payloadJson, string field)
    {
        JsonObject root;
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            root = new JsonObject();
        }
        else
        {
            try
            {
                root = JsonNode.Parse(payloadJson) as JsonObject ?? new JsonObject();
            }
            catch
            {
                root = new JsonObject();
            }
        }

        root.Remove(field);
        return root.ToJsonString();
    }

    private JobPreviewResult BuildPreviewForJob(
        RunJobRecord job,
        IReadOnlyList<TemplateRecord> activeTemplates,
        AppSettingsDto settings,
        DateTime nowUtc)
    {
        var messageOverrideText = TryExtractPayloadString(job.PayloadJson, PayloadFieldMessageOverride);
        if (!string.IsNullOrWhiteSpace(messageOverrideText))
        {
            var totalRawOverride = TryExtractPayloadTotal(job.PayloadJson);
            var approxDebtTextOverride = RuleEngineService.BuildApproxDebtText(job.PayloadJson, settings.DebtBufferAmount);
            var templateOverride = job.TemplateId.HasValue && job.TemplateId.Value > 0
                ? activeTemplates.FirstOrDefault(x => x.Id == job.TemplateId.Value)
                : null;

            var variablesJsonOverride = JsonSerializer.Serialize(new
            {
                fullFio = (job.ClientFio ?? string.Empty).Trim(),
                totalWithCommissionRaw = totalRawOverride,
                approxDebtText = approxDebtTextOverride,
                templateId = templateOverride?.Id,
                templateKind = templateOverride?.Kind ?? string.Empty,
                templateName = templateOverride?.Name ?? string.Empty,
                messageOverride = true
            });

            return new JobPreviewResult
            {
                Status = PreviewStatusReady,
                Text = messageOverrideText,
                VariablesJson = variablesJsonOverride,
                UpdatedAtUtc = nowUtc,
                ErrorCode = string.Empty,
                ErrorDetail = string.Empty,
                TemplateId = templateOverride?.Id,
                TemplateName = templateOverride?.Name ?? string.Empty,
                TemplateKind = templateOverride?.Kind ?? string.Empty
            };
        }

        var rendered = ruleEngine.BuildDispatchMessage(activeTemplates, job, settings.DebtBufferAmount);
        if (!string.IsNullOrWhiteSpace(rendered.ErrorCode))
        {
            return new JobPreviewResult
            {
                Status = PreviewStatusError,
                Text = string.Empty,
                VariablesJson = "{}",
                UpdatedAtUtc = nowUtc,
                ErrorCode = rendered.ErrorCode,
                ErrorDetail = rendered.ErrorMessage,
                TemplateId = null,
                TemplateName = string.Empty,
                TemplateKind = string.Empty
            };
        }

        var template = rendered.TemplateId.HasValue && rendered.TemplateId.Value > 0
            ? activeTemplates.FirstOrDefault(x => x.Id == rendered.TemplateId.Value)
            : null;

        var totalRaw = TryExtractPayloadTotal(job.PayloadJson);
        var approxDebtText = string.IsNullOrWhiteSpace(totalRaw)
            ? string.Empty
            : RuleEngineService.BuildApproxDebtText(job.PayloadJson, settings.DebtBufferAmount);
        var status = string.IsNullOrWhiteSpace(totalRaw)
            ? PreviewStatusNeedsDebt
            : PreviewStatusReady;

        var errorCode = status == PreviewStatusNeedsDebt
            ? "PREVIEW_DEBT_MISSING"
            : string.Empty;
        var errorDetail = status == PreviewStatusNeedsDebt
            ? "Точная сумма долга пока не загружена. Превью станет доступно после обновления суммы долга."
            : string.Empty;

        var variablesJson = JsonSerializer.Serialize(new
        {
            fullFio = (job.ClientFio ?? string.Empty).Trim(),
            totalWithCommissionRaw = totalRaw,
            approxDebtText,
            templateId = rendered.TemplateId,
            templateKind = template?.Kind ?? string.Empty,
            templateName = template?.Name ?? string.Empty
        });

        return new JobPreviewResult
        {
            Status = status,
            Text = status == PreviewStatusNeedsDebt ? string.Empty : rendered.MessageText,
            VariablesJson = variablesJson,
            UpdatedAtUtc = nowUtc,
            ErrorCode = errorCode,
            ErrorDetail = errorDetail,
            TemplateId = rendered.TemplateId,
            TemplateName = template?.Name ?? string.Empty,
            TemplateKind = template?.Kind ?? string.Empty
        };
    }

    private static void ApplyPreviewToJob(RunJobRecord job, JobPreviewResult preview)
    {
        job.PreviewStatus = preview.Status;
        job.PreviewText = preview.Text;
        job.PreviewVariablesJson = preview.VariablesJson;
        job.PreviewUpdatedAtUtc = preview.UpdatedAtUtc;
        job.PreviewErrorCode = preview.ErrorCode;
        job.PreviewErrorDetail = preview.ErrorDetail;
    }

    private static QueueJobMessageOverrideDto BuildMessageOverrideDto(
        RunJobRecord job,
        bool hasMessageOverride,
        string messageOverrideText,
        JobPreviewResult preview)
    {
        return new QueueJobMessageOverrideDto
        {
            JobId = job.Id,
            RunSessionId = job.RunSessionId,
            ExternalClientId = job.ExternalClientId,
            Phone = job.Phone,
            HasMessageOverride = hasMessageOverride,
            MessageOverrideText = messageOverrideText,
            Preview = new QueueJobPreviewDto
            {
                JobId = job.Id,
                RunSessionId = job.RunSessionId,
                ExternalClientId = job.ExternalClientId,
                Phone = job.Phone,
                TemplateId = preview.TemplateId,
                TemplateName = preview.TemplateName,
                TemplateKind = preview.TemplateKind,
                Status = preview.Status,
                Text = preview.Text,
                VariablesJson = preview.VariablesJson,
                UpdatedAtUtc = preview.UpdatedAtUtc,
                ErrorCode = preview.ErrorCode,
                ErrorDetail = preview.ErrorDetail
            }
        };
    }

    private static string BuildApproxDebtText(string exactTotalRaw, int debtBufferAmount)
    {
        if (string.IsNullOrWhiteSpace(exactTotalRaw))
        {
            return string.Empty;
        }

        var payload = JsonSerializer.Serialize(new
        {
            totalWithCommissionRaw = exactTotalRaw
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

    private static string NormalizePhone(string? rawPhone)
    {
        return PhoneNormalizer.Normalize(rawPhone);
    }

}
