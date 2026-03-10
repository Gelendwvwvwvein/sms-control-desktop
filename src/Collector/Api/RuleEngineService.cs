using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class RuleEngineService
{
    private const string TemplateStatusActive = "active";
    public const int DefaultDebtBufferAmount = 2000;
    public const int MaxDebtBufferAmount = 1_000_000;

    public async Task<List<TemplateRecord>> GetActiveTemplatesAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        return await db.Templates
            .AsNoTracking()
            .Where(x => x.Status == TemplateStatusActive)
            .OrderBy(x => x.Id)
            .ToListAsync(cancellationToken);
    }

    public long? ResolveAutoTemplateId(IReadOnlyList<TemplateRecord> activeTemplates, int daysOverdue)
    {
        var template = ResolveAutoTemplate(activeTemplates, daysOverdue);
        return template?.Id;
    }

    public RuleEngineMessageResult BuildDispatchMessage(
        IReadOnlyList<TemplateRecord> activeTemplates,
        RunJobRecord job,
        int debtBufferAmount = DefaultDebtBufferAmount)
    {
        var fio = string.IsNullOrWhiteSpace(job.ClientFio) ? "Клиент" : job.ClientFio.Trim();
        var debtText = BuildApproxDebtText(job.PayloadJson, debtBufferAmount);

        TemplateRecord? selected = null;
        if (job.TemplateId.HasValue && job.TemplateId.Value > 0)
        {
            selected = activeTemplates.FirstOrDefault(x => x.Id == job.TemplateId.Value);
        }

        if (selected is null)
        {
            selected = ResolveAutoTemplate(activeTemplates, job.DaysOverdue);
        }

        if (selected is null)
        {
            return new RuleEngineMessageResult
            {
                TemplateId = null,
                MessageText = string.Empty,
                UsedFallback = false,
                ErrorCode = "TEMPLATE_NOT_RESOLVED",
                ErrorMessage = "Не найден активный шаблон для текущей просрочки или выбранного templateId."
            };
        }

        var rendered = RenderTemplateText(selected.Text, fio, debtText);
        if (string.IsNullOrWhiteSpace(rendered))
        {
            return new RuleEngineMessageResult
            {
                TemplateId = null,
                MessageText = string.Empty,
                UsedFallback = false,
                ErrorCode = "TEMPLATE_RENDER_EMPTY",
                ErrorMessage = $"Шаблон id={selected.Id} после подстановки переменных дал пустой текст."
            };
        }

        return new RuleEngineMessageResult
        {
            TemplateId = selected.Id,
            MessageText = rendered,
            UsedFallback = false
        };
    }

    public static string BuildApproxDebtText(
        string? payloadJson,
        int debtBufferAmount = DefaultDebtBufferAmount)
    {
        var raw = ExtractTotalWithCommissionRaw(payloadJson);
        if (!TryParseAmount(raw, out var amount))
        {
            return string.Empty;
        }

        var withBuffer = amount + Math.Clamp(debtBufferAmount, 0, MaxDebtBufferAmount);
        var rounded = RoundToNearestThousandMidpointUp(withBuffer);
        return $"{rounded:0}р";
    }

    private static TemplateRecord? ResolveAutoTemplate(IReadOnlyList<TemplateRecord> activeTemplates, int daysOverdue)
    {
        return activeTemplates
            .Where(x => TemplateService.IsTemplateEligibleForOverdue(x, daysOverdue))
            .OrderBy(x => GetRulePriority(x))
            .ThenBy(x => x.Id)
            .FirstOrDefault();
    }

    private static int GetRulePriority(TemplateRecord template)
    {
        var mode = (template.OverdueMode ?? string.Empty).Trim().ToLowerInvariant();
        if (mode == TemplateService.OverdueModeExact)
        {
            return 0;
        }

        if (template.OverdueFromDays.HasValue && template.OverdueToDays.HasValue)
        {
            return Math.Max(1, template.OverdueToDays.Value - template.OverdueFromDays.Value + 1);
        }

        return int.MaxValue;
    }

    private static string RenderTemplateText(string templateText, string fullFio, string debtText)
    {
        var text = templateText ?? string.Empty;
        text = ReplaceToken(text, "{полное_фио}", fullFio);
        text = ReplaceToken(text, "{сумма_долга}", debtText);
        return text.Trim();
    }

    private static string ReplaceToken(string text, string token, string value)
    {
        return Regex.Replace(
            text,
            Regex.Escape(token),
            value,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static string ExtractTotalWithCommissionRaw(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
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

            if (!doc.RootElement.TryGetProperty(PayloadFields.TotalWithCommissionRaw, out var totalNode))
            {
                return string.Empty;
            }

            if (totalNode.ValueKind == JsonValueKind.String)
            {
                return totalNode.GetString() ?? string.Empty;
            }

            if (totalNode.ValueKind == JsonValueKind.Number)
            {
                return totalNode.GetRawText();
            }
        }
        catch
        {
            return string.Empty;
        }

        return string.Empty;
    }

    private static bool TryParseAmount(string? raw, out decimal amount)
    {
        amount = 0m;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var prepared = (raw ?? string.Empty)
            .Replace('\u00A0', ' ')
            .Replace("₽", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("руб", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Replace("р", string.Empty, StringComparison.OrdinalIgnoreCase)
            .Trim();

        prepared = prepared.Replace(" ", string.Empty).Replace(",", ".");

        return decimal.TryParse(
            prepared,
            NumberStyles.AllowDecimalPoint | NumberStyles.AllowLeadingSign,
            CultureInfo.InvariantCulture,
            out amount);
    }

    private static decimal RoundToNearestThousandMidpointUp(decimal amount)
    {
        var thousands = amount / 1000m;
        var floor = Math.Floor(thousands);
        var fractional = thousands - floor;

        var roundedThousands = fractional switch
        {
            > 0.5m => floor + 1m,
            < 0.5m => floor,
            _ => floor + 1m
        };

        return roundedThousands * 1000m;
    }
}

public sealed class RuleEngineMessageResult
{
    public long? TemplateId { get; init; }
    public string MessageText { get; init; } = string.Empty;
    public bool UsedFallback { get; init; }
    public string ErrorCode { get; init; } = string.Empty;
    public string ErrorMessage { get; init; } = string.Empty;
}
