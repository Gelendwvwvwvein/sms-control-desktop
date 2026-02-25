using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class TemplateService
{
    public const string OverdueModeRange = "range";
    public const string OverdueModeExact = "exact";

    private const string StatusDraft = "draft";
    private const string StatusActive = "active";
    private const string KindFallback = "custom";

    private static readonly HashSet<string> RequiredTokens = new(StringComparer.Ordinal)
    {
        "{полное_фио}",
        "{сумма_долга}"
    };

    private static readonly HashSet<string> AllowedStatuses = new(StringComparer.OrdinalIgnoreCase)
    {
        StatusDraft,
        StatusActive
    };

    private static readonly HashSet<string> AllowedOverdueModes = new(StringComparer.OrdinalIgnoreCase)
    {
        OverdueModeRange,
        OverdueModeExact
    };

    private static readonly IReadOnlyDictionary<string, LegacyTemplateDefaults> LegacyDefaultsByKind =
        new Dictionary<string, LegacyTemplateDefaults>(StringComparer.OrdinalIgnoreCase)
        {
            ["sms1"] = new LegacyTemplateDefaults("СМС 1", "Старый тип шаблона", 3, 5, true, "смс2", 10),
            ["sms1_regular"] = new LegacyTemplateDefaults("СМС 1 (постоянный клиент)", "Старый тип шаблона", 3, 5, false, "смс2", 20),
            ["sms2"] = new LegacyTemplateDefaults("СМС 2", "Старый тип шаблона", 6, 20, true, "смс2", 30),
            ["sms3"] = new LegacyTemplateDefaults("СМС 3", "Старый тип шаблона", 21, 29, true, "смс3", 40),
            ["ka1"] = new LegacyTemplateDefaults("СМС от КА1", "Старый тип шаблона", 30, 45, true, "смс от ка", 50),
            ["ka2"] = new LegacyTemplateDefaults("СМС от КА2", "Старый тип шаблона", 46, 50, true, "смс ка{n}", 60),
            ["ka_final"] = new LegacyTemplateDefaults("СМС от КА (финал)", "Старый тип шаблона", 51, 59, true, "смс ка фин", 70)
        };

    private static readonly IReadOnlyList<TemplateTypeMetaDto> TypeMeta =
        LegacyDefaultsByKind
            .Select(x => new TemplateTypeMetaDto
            {
                Kind = x.Key,
                Label = x.Value.Label,
                Description = x.Value.Description,
                RuleHint = $"Старый preset: {x.Value.RangeFrom}-{x.Value.RangeTo} дней.",
                SortOrder = x.Value.SortOrder
            })
            .OrderBy(x => x.SortOrder)
            .ToList();

    public static IReadOnlyList<TemplateTypeMetaDto> GetMeta() => TypeMeta;

    public static bool IsTemplateEligibleForOverdue(TemplateRecord template, int daysOverdue, bool allowManualOnly = false)
    {
        if (!template.AutoAssign)
        {
            return allowManualOnly;
        }

        return IsOverdueRuleMatch(
            template.OverdueMode,
            template.OverdueFromDays,
            template.OverdueToDays,
            template.OverdueExactDay,
            daysOverdue);
    }

    public async Task<List<TemplateDto>> ListAsync(AppDbContext db, string? status, CancellationToken cancellationToken)
    {
        var normalizedStatus = NormalizeStatus(status, allowEmpty: true);
        IQueryable<TemplateRecord> query = db.Templates.AsNoTracking();

        if (!string.IsNullOrWhiteSpace(normalizedStatus))
        {
            query = query.Where(x => x.Status == normalizedStatus);
        }

        var records = await query.ToListAsync(cancellationToken);

        return records
            .OrderBy(x => GetSortOrder(NormalizeKind(x.Kind)))
            .ThenBy(x => x.Id)
            .Select(MapToDto)
            .ToList();
    }

    public async Task<TemplateDto?> GetByIdAsync(AppDbContext db, long id, CancellationToken cancellationToken)
    {
        var record = await db.Templates
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);

        return record is null ? null : MapToDto(record);
    }

    public async Task<TemplateDto> CreateAsync(AppDbContext db, TemplateUpsertRequest payload, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var normalizedKind = NormalizeKind(payload.Kind);
        var rule = ResolveOverdueRule(payload, normalizedKind);
        var resolvedAutoAssign = ResolveAutoAssign(payload.AutoAssign, normalizedKind);
        var record = new TemplateRecord
        {
            Name = payload.Name.Trim(),
            Kind = normalizedKind,
            OverdueMode = rule.Mode,
            OverdueFromDays = rule.FromDays,
            OverdueToDays = rule.ToDays,
            OverdueExactDay = rule.ExactDay,
            AutoAssign = resolvedAutoAssign,
            CommentText = NormalizeCommentText(payload.CommentText, normalizedKind),
            Status = NormalizeStatus(payload.Status),
            Text = payload.Text.Trim(),
            CreatedAtUtc = now,
            UpdatedAtUtc = now
        };

        db.Templates.Add(record);
        await db.SaveChangesAsync(cancellationToken);

        return MapToDto(record);
    }

    public async Task<TemplateDto?> UpdateAsync(AppDbContext db, long id, TemplateUpsertRequest payload, CancellationToken cancellationToken)
    {
        var record = await db.Templates.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return null;

        var normalizedKind = NormalizeKind(payload.Kind);
        var rule = ResolveOverdueRule(payload, normalizedKind);
        var resolvedAutoAssign = ResolveAutoAssign(payload.AutoAssign, normalizedKind);

        record.Name = payload.Name.Trim();
        record.Kind = normalizedKind;
        record.OverdueMode = rule.Mode;
        record.OverdueFromDays = rule.FromDays;
        record.OverdueToDays = rule.ToDays;
        record.OverdueExactDay = rule.ExactDay;
        record.AutoAssign = resolvedAutoAssign;
        record.CommentText = NormalizeCommentText(payload.CommentText, normalizedKind);
        record.Status = NormalizeStatus(payload.Status);
        record.Text = payload.Text.Trim();
        record.UpdatedAtUtc = DateTime.UtcNow;

        db.Templates.Update(record);
        await db.SaveChangesAsync(cancellationToken);

        return MapToDto(record);
    }

    public async Task<TemplateDto?> UpdateStatusAsync(AppDbContext db, long id, string status, CancellationToken cancellationToken)
    {
        var record = await db.Templates.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return null;

        record.Status = NormalizeStatus(status);
        record.UpdatedAtUtc = DateTime.UtcNow;

        db.Templates.Update(record);
        await db.SaveChangesAsync(cancellationToken);

        return MapToDto(record);
    }

    public static ApiErrorDto? ValidateUpsertRequest(TemplateUpsertRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Name))
        {
            return new ApiErrorDto { Code = "TEMPLATE_NAME_REQUIRED", Message = "Название шаблона обязательно." };
        }

        if (string.IsNullOrWhiteSpace(payload.Status))
        {
            return new ApiErrorDto { Code = "TEMPLATE_STATUS_REQUIRED", Message = "Статус шаблона обязателен." };
        }

        var normalizedStatus = payload.Status.Trim().ToLowerInvariant();
        if (!AllowedStatuses.Contains(normalizedStatus))
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_STATUS_INVALID",
                Message = "Статус шаблона должен быть 'draft' или 'active'."
            };
        }

        if (string.IsNullOrWhiteSpace(payload.Text))
        {
            return new ApiErrorDto { Code = "TEMPLATE_TEXT_REQUIRED", Message = "Текст шаблона обязателен." };
        }

        var tokenValidationError = ValidateTextTokens(payload.Text);
        if (tokenValidationError is not null)
        {
            return tokenValidationError;
        }

        var normalizedKind = NormalizeKind(payload.Kind);
        var modeValidation = ValidateOverdueRule(payload, normalizedKind);
        if (modeValidation is not null)
        {
            return modeValidation;
        }

        var normalizedComment = NormalizeCommentText(payload.CommentText, normalizedKind);
        if (normalizedStatus == StatusActive && string.IsNullOrWhiteSpace(normalizedComment))
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_COMMENT_REQUIRED",
                Message = "Для активного шаблона заполните комментарий, который пишется в договор после отправки."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateStatusPatchRequest(TemplateStatusPatchRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Status))
        {
            return new ApiErrorDto { Code = "TEMPLATE_STATUS_REQUIRED", Message = "Статус шаблона обязателен." };
        }

        if (!AllowedStatuses.Contains(payload.Status.Trim()))
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_STATUS_INVALID",
                Message = "Статус шаблона должен быть 'draft' или 'active'."
            };
        }

        return null;
    }

    public static bool IsValidStatusFilter(string? status)
    {
        if (string.IsNullOrWhiteSpace(status)) return true;
        return AllowedStatuses.Contains(status.Trim());
    }

    private static ApiErrorDto? ValidateTextTokens(string text)
    {
        var matches = TemplateTokenRegex().Matches(text);
        var tokens = matches
            .Select(x => x.Value.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var token in tokens)
        {
            if (!RequiredTokens.Contains(token))
            {
                return new ApiErrorDto
                {
                    Code = "TEMPLATE_TOKEN_UNSUPPORTED",
                    Message = $"Недопустимая переменная в шаблоне: {token}. Разрешены только {{полное_фио}} и {{сумма_долга}}."
                };
            }
        }

        var missing = RequiredTokens.Where(x => !tokens.Contains(x, StringComparer.Ordinal)).ToList();
        if (missing.Count > 0)
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_REQUIRED_TOKEN_MISSING",
                Message = $"В шаблоне должны присутствовать переменные: {string.Join(", ", RequiredTokens)}."
            };
        }

        return null;
    }

    private static ApiErrorDto? ValidateOverdueRule(TemplateUpsertRequest payload, string normalizedKind)
    {
        try
        {
            _ = ResolveOverdueRule(payload, normalizedKind);
            return null;
        }
        catch (ArgumentException ex)
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_OVERDUE_RULE_INVALID",
                Message = ex.Message
            };
        }
    }

    private static OverdueRule ResolveOverdueRule(TemplateUpsertRequest payload, string normalizedKind)
    {
        var from = payload.OverdueFromDays;
        var to = payload.OverdueToDays;
        var exact = payload.OverdueExactDay;

        var mode = NormalizeOverdueMode(payload.OverdueMode, allowEmpty: true);
        if (string.IsNullOrWhiteSpace(mode))
        {
            mode = exact.HasValue ? OverdueModeExact : (from.HasValue || to.HasValue ? OverdueModeRange : string.Empty);
        }

        if (string.IsNullOrWhiteSpace(mode))
        {
            var defaults = TryGetLegacyDefaults(normalizedKind);
            if (defaults is not null)
            {
                return new OverdueRule(OverdueModeRange, defaults.RangeFrom, defaults.RangeTo, null);
            }

            throw new ArgumentException("Укажите правило просрочки: диапазон или точный день.");
        }

        if (!AllowedOverdueModes.Contains(mode))
        {
            throw new ArgumentException("Режим правила просрочки должен быть 'range' или 'exact'.");
        }

        if (mode == OverdueModeExact)
        {
            if (!exact.HasValue)
            {
                throw new ArgumentException("Для режима 'exact' нужно указать точный день просрочки.");
            }

            if (exact.Value < 0)
            {
                throw new ArgumentException("Точный день просрочки должен быть числом >= 0.");
            }

            return new OverdueRule(OverdueModeExact, null, null, exact.Value);
        }

        if (!from.HasValue || !to.HasValue)
        {
            var defaults = TryGetLegacyDefaults(normalizedKind);
            if (defaults is not null)
            {
                return new OverdueRule(OverdueModeRange, defaults.RangeFrom, defaults.RangeTo, null);
            }

            throw new ArgumentException("Для режима 'range' нужно указать оба значения: от и до.");
        }

        if (from.Value < 0 || to.Value < 0)
        {
            throw new ArgumentException("Диапазон просрочки должен быть в пределах >= 0.");
        }

        if (to.Value < from.Value)
        {
            throw new ArgumentException("В диапазоне просрочки значение 'до' не может быть меньше значения 'от'.");
        }

        return new OverdueRule(OverdueModeRange, from.Value, to.Value, null);
    }

    private static int GetSortOrder(string kind)
    {
        if (TryGetLegacyDefaults(kind) is { } defaults)
        {
            return defaults.SortOrder;
        }

        return int.MaxValue;
    }

    private static TemplateDto MapToDto(TemplateRecord record)
    {
        var kind = NormalizeKind(record.Kind);
        var mode = NormalizeOverdueMode(record.OverdueMode);
        var defaults = TryGetLegacyDefaults(kind);
        var fromDays = mode == OverdueModeRange
            ? record.OverdueFromDays ?? defaults?.RangeFrom
            : null;
        var toDays = mode == OverdueModeRange
            ? record.OverdueToDays ?? defaults?.RangeTo
            : null;
        var exactDay = mode == OverdueModeExact
            ? record.OverdueExactDay
            : null;
        var commentText = NormalizeCommentText(record.CommentText, kind);

        return new TemplateDto
        {
            Id = record.Id,
            Name = record.Name,
            Kind = kind,
            KindLabel = defaults?.Label ?? kind,
            OverdueMode = mode,
            OverdueFromDays = fromDays,
            OverdueToDays = toDays,
            OverdueExactDay = exactDay,
            OverdueText = BuildOverdueText(mode, fromDays, toDays, exactDay),
            AutoAssign = record.AutoAssign,
            CommentText = commentText,
            Status = NormalizeStatus(record.Status),
            Text = record.Text,
            CreatedAtUtc = record.CreatedAtUtc,
            UpdatedAtUtc = record.UpdatedAtUtc
        };
    }

    private static string BuildOverdueText(
        string mode,
        int? fromDays,
        int? toDays,
        int? exactDay)
    {
        if (string.Equals(mode, OverdueModeExact, StringComparison.OrdinalIgnoreCase) && exactDay.HasValue)
        {
            return $"Точный день: {exactDay.Value}";
        }

        if (fromDays.HasValue && toDays.HasValue)
        {
            return $"Диапазон: {fromDays.Value}-{toDays.Value}";
        }

        return "Не задано";
    }

    private static bool IsOverdueRuleMatch(
        string? modeRaw,
        int? fromDays,
        int? toDays,
        int? exactDay,
        int daysOverdue)
    {
        var mode = NormalizeOverdueMode(modeRaw);
        if (mode == OverdueModeExact)
        {
            return exactDay.HasValue && exactDay.Value == daysOverdue;
        }

        if (!fromDays.HasValue || !toDays.HasValue)
        {
            return false;
        }

        return daysOverdue >= fromDays.Value && daysOverdue <= toDays.Value;
    }

    private static LegacyTemplateDefaults? TryGetLegacyDefaults(string kind)
    {
        return LegacyDefaultsByKind.TryGetValue(kind, out var defaults) ? defaults : null;
    }

    private static string NormalizeKind(string? kind)
    {
        var value = (kind ?? string.Empty).Trim().ToLowerInvariant();
        return string.IsNullOrWhiteSpace(value) ? KindFallback : value;
    }

    private static string NormalizeCommentText(string? value, string normalizedKind)
    {
        var text = (value ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(text))
        {
            return text;
        }

        return TryGetLegacyDefaults(normalizedKind)?.DefaultComment ?? string.Empty;
    }

    private static bool ResolveAutoAssign(bool? autoAssign, string normalizedKind)
    {
        if (autoAssign.HasValue)
        {
            return autoAssign.Value;
        }

        return TryGetLegacyDefaults(normalizedKind)?.AutoAssign ?? true;
    }

    private static string NormalizeOverdueMode(string? mode, bool allowEmpty = false)
    {
        var value = (mode ?? string.Empty).Trim().ToLowerInvariant();
        if (allowEmpty && string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        return value == OverdueModeExact ? OverdueModeExact : OverdueModeRange;
    }

    private static string NormalizeStatus(string? status, bool allowEmpty = false)
    {
        var value = (status ?? string.Empty).Trim().ToLowerInvariant();
        if (allowEmpty && string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        if (!AllowedStatuses.Contains(value))
        {
            return StatusDraft;
        }

        return value;
    }

    [GeneratedRegex(@"\{[^{}]+\}", RegexOptions.CultureInvariant)]
    private static partial Regex TemplateTokenRegex();

    private sealed class LegacyTemplateDefaults(
        string label,
        string description,
        int rangeFrom,
        int rangeTo,
        bool autoAssign,
        string defaultComment,
        int sortOrder)
    {
        public string Label { get; } = label;
        public string Description { get; } = description;
        public int RangeFrom { get; } = rangeFrom;
        public int RangeTo { get; } = rangeTo;
        public bool AutoAssign { get; } = autoAssign;
        public string DefaultComment { get; } = defaultComment;
        public int SortOrder { get; } = sortOrder;
    }

    private readonly record struct OverdueRule(
        string Mode,
        int? FromDays,
        int? ToDays,
        int? ExactDay);
}
