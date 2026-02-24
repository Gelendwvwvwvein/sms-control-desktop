using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class TemplateService
{
    private const string StatusDraft = "draft";
    private const string StatusActive = "active";

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

    private static readonly IReadOnlyList<TemplateTypeMetaDto> TypeMeta =
    [
        new TemplateTypeMetaDto
        {
            Kind = "sms1",
            Label = "СМС 1",
            RangeText = "3-5 дней",
            MinOverdueDays = 3,
            MaxOverdueDays = 5,
            AutoAssign = true,
            RuleHint = "Первичное сообщение: только клиенты с просрочкой 3-5 дней.",
            SortOrder = 10
        },
        new TemplateTypeMetaDto
        {
            Kind = "sms1_regular",
            Label = "СМС 1 (постоянный клиент)",
            RangeText = "3-5 дней",
            MinOverdueDays = 3,
            MaxOverdueDays = 5,
            AutoAssign = false,
            RuleHint = "Вариант для постоянных клиентов. В автоподбор не включается, назначается вручную.",
            SortOrder = 20
        },
        new TemplateTypeMetaDto
        {
            Kind = "sms2",
            Label = "СМС 2",
            RangeText = "6-20 дней",
            MinOverdueDays = 6,
            MaxOverdueDays = 20,
            AutoAssign = true,
            RuleHint = "Повторное сообщение: для клиентов с просрочкой 6-20 дней.",
            SortOrder = 30
        },
        new TemplateTypeMetaDto
        {
            Kind = "sms3",
            Label = "СМС 3",
            RangeText = "21-29 дней",
            MinOverdueDays = 21,
            MaxOverdueDays = 29,
            AutoAssign = true,
            RuleHint = "Третье сообщение до этапа КА: для клиентов с просрочкой 21-29 дней.",
            SortOrder = 40
        },
        new TemplateTypeMetaDto
        {
            Kind = "ka1",
            Label = "СМС от КА1",
            RangeText = "30-45 дней",
            MinOverdueDays = 30,
            MaxOverdueDays = 45,
            AutoAssign = true,
            RuleHint = "Первое сообщение от КА: клиенты с просрочкой 30-45 дней.",
            SortOrder = 50
        },
        new TemplateTypeMetaDto
        {
            Kind = "ka2",
            Label = "СМС от КА2",
            RangeText = "46-50 дней",
            MinOverdueDays = 46,
            MaxOverdueDays = 50,
            AutoAssign = true,
            RuleHint = "Повторное сообщение от КА: клиенты с просрочкой 46-50 дней.",
            SortOrder = 60
        },
        new TemplateTypeMetaDto
        {
            Kind = "ka_final",
            Label = "СМС от КА (финал)",
            RangeText = "51-59 дней",
            MinOverdueDays = 51,
            MaxOverdueDays = 59,
            AutoAssign = true,
            RuleHint = "Финальное сообщение КА: клиенты с просрочкой 51-59 дней.",
            SortOrder = 70
        }
    ];

    private static readonly IReadOnlyDictionary<string, TemplateTypeMetaDto> TypeMetaByKind =
        TypeMeta.ToDictionary(x => x.Kind, StringComparer.OrdinalIgnoreCase);

    public static IReadOnlyList<TemplateTypeMetaDto> GetMeta() => TypeMeta;

    public static bool IsTemplateEligibleForOverdue(string? kind, int daysOverdue)
    {
        var normalizedKind = (kind ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrEmpty(normalizedKind)) return false;
        if (!TypeMetaByKind.TryGetValue(normalizedKind, out var meta)) return false;
        return daysOverdue >= meta.MinOverdueDays && daysOverdue <= meta.MaxOverdueDays;
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
        var record = new TemplateRecord
        {
            Name = payload.Name.Trim(),
            Kind = NormalizeKind(payload.Kind),
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

        record.Name = payload.Name.Trim();
        record.Kind = NormalizeKind(payload.Kind);
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

        if (string.IsNullOrWhiteSpace(payload.Kind))
        {
            return new ApiErrorDto { Code = "TEMPLATE_KIND_REQUIRED", Message = "Тип шаблона обязателен." };
        }

        var normalizedKind = NormalizeKind(payload.Kind);
        if (!TypeMetaByKind.ContainsKey(normalizedKind))
        {
            return new ApiErrorDto
            {
                Code = "TEMPLATE_KIND_INVALID",
                Message = $"Неизвестный тип шаблона: '{payload.Kind}'."
            };
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

        if (string.IsNullOrWhiteSpace(payload.Text))
        {
            return new ApiErrorDto { Code = "TEMPLATE_TEXT_REQUIRED", Message = "Текст шаблона обязателен." };
        }

        var tokenValidationError = ValidateTextTokens(payload.Text);
        if (tokenValidationError is not null)
        {
            return tokenValidationError;
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

    private static int GetSortOrder(string kind)
    {
        if (TypeMetaByKind.TryGetValue(kind, out var meta))
        {
            return meta.SortOrder;
        }

        return int.MaxValue;
    }

    private static TemplateDto MapToDto(TemplateRecord record)
    {
        var kind = NormalizeKind(record.Kind);
        TypeMetaByKind.TryGetValue(kind, out var meta);

        return new TemplateDto
        {
            Id = record.Id,
            Name = record.Name,
            Kind = kind,
            KindLabel = meta?.Label ?? kind,
            RangeText = meta?.RangeText ?? string.Empty,
            MinOverdueDays = meta?.MinOverdueDays ?? 0,
            MaxOverdueDays = meta?.MaxOverdueDays ?? 0,
            AutoAssign = meta?.AutoAssign ?? false,
            Status = NormalizeStatus(record.Status),
            Text = record.Text,
            CreatedAtUtc = record.CreatedAtUtc,
            UpdatedAtUtc = record.UpdatedAtUtc
        };
    }

    private static string NormalizeKind(string? kind)
    {
        return (kind ?? string.Empty).Trim().ToLowerInvariant();
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
}
