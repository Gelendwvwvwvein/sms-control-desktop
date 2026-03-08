using System.Text.Json;
using System.Globalization;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class SettingsStore
{
    private const string AppSettingsKey = "app_settings";
    private const string OverdueModeRange = "range";
    private const string OverdueModeExact = "exact";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);
    private static readonly IReadOnlyList<TemplateRuleTypeDto> DefaultTemplateRuleTypes =
    [
        new() { Id = "sms1", Name = "СМС 1", OverdueMode = OverdueModeRange, OverdueFromDays = 3, OverdueToDays = 5, OverdueExactDay = null, AutoAssign = true, SortOrder = 10 },
        new() { Id = "sms1_regular", Name = "СМС 1 (постоянный клиент)", OverdueMode = OverdueModeRange, OverdueFromDays = 3, OverdueToDays = 5, OverdueExactDay = null, AutoAssign = false, SortOrder = 20 },
        new() { Id = "sms2", Name = "СМС 2", OverdueMode = OverdueModeRange, OverdueFromDays = 6, OverdueToDays = 20, OverdueExactDay = null, AutoAssign = true, SortOrder = 30 },
        new() { Id = "sms3", Name = "СМС 3", OverdueMode = OverdueModeRange, OverdueFromDays = 21, OverdueToDays = 29, OverdueExactDay = null, AutoAssign = true, SortOrder = 40 },
        new() { Id = "ka1", Name = "СМС от КА1", OverdueMode = OverdueModeRange, OverdueFromDays = 30, OverdueToDays = 45, OverdueExactDay = null, AutoAssign = true, SortOrder = 50 },
        new() { Id = "ka2", Name = "СМС от КА2", OverdueMode = OverdueModeRange, OverdueFromDays = 46, OverdueToDays = 50, OverdueExactDay = null, AutoAssign = true, SortOrder = 60 },
        new() { Id = "ka_final", Name = "СМС от КА (финал)", OverdueMode = OverdueModeRange, OverdueFromDays = 51, OverdueToDays = 59, OverdueExactDay = null, AutoAssign = true, SortOrder = 70 }
    ];

    public async Task<AppSettingsDto> GetAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var row = await db.Settings.FirstOrDefaultAsync(x => x.Key == AppSettingsKey, cancellationToken);
        if (row is null || string.IsNullOrWhiteSpace(row.ValueJson))
        {
            return new AppSettingsDto();
        }

        try
        {
            var parsed = JsonSerializer.Deserialize<AppSettingsDto>(row.ValueJson, JsonOptions);
            return Normalize(parsed ?? new AppSettingsDto());
        }
        catch
        {
            return new AppSettingsDto();
        }
    }

    public async Task<AppSettingsDto> SaveAsync(AppDbContext db, AppSettingsDto input, CancellationToken cancellationToken)
    {
        var normalized = Normalize(input);
        await SyncTemplatesByRuleTypesAsync(db, normalized.TemplateRuleTypes, cancellationToken);
        var payload = JsonSerializer.Serialize(normalized, JsonOptions);

        var row = await db.Settings.FirstOrDefaultAsync(x => x.Key == AppSettingsKey, cancellationToken);
        if (row is null)
        {
            row = new SettingRecord
            {
                Key = AppSettingsKey,
                ValueJson = payload,
                UpdatedAtUtc = DateTime.UtcNow
            };
            db.Settings.Add(row);
        }
        else
        {
            row.ValueJson = payload;
            row.UpdatedAtUtc = DateTime.UtcNow;
        }

        await db.SaveChangesAsync(cancellationToken);
        return normalized;
    }

    public async Task<CommentRulesDto> SaveCommentRulesAsync(AppDbContext db, CommentRulesDto rules, CancellationToken cancellationToken)
    {
        var settings = await GetAsync(db, cancellationToken);
        settings.CommentRules = rules ?? new CommentRulesDto();
        var saved = await SaveAsync(db, settings, cancellationToken);
        return saved.CommentRules;
    }

    private static AppSettingsDto Normalize(AppSettingsDto settings)
    {
        settings.LoginUrl = (settings.LoginUrl ?? string.Empty).Trim();
        settings.Login = (settings.Login ?? string.Empty).Trim();
        settings.Password = settings.Password ?? string.Empty;
        settings.Gap = settings.Gap <= 0 ? 8 : settings.Gap;
        settings.RecentSmsCooldownDays = Math.Clamp(settings.RecentSmsCooldownDays, 0, 365);
        settings.WorkWindowStart = NormalizeTimeOrDefault(settings.WorkWindowStart, "08:00");
        settings.WorkWindowEnd = NormalizeTimeOrDefault(settings.WorkWindowEnd, "21:00");

        settings.CommentRules ??= new CommentRulesDto();
        settings.CommentRules.Sms2 = CleanOrDefault(settings.CommentRules.Sms2, "смс2");
        settings.CommentRules.Sms3 = CleanOrDefault(settings.CommentRules.Sms3, "смс3");
        settings.CommentRules.Ka1 = CleanOrDefault(settings.CommentRules.Ka1, "смс от ка");
        settings.CommentRules.KaN = CleanOrDefault(settings.CommentRules.KaN, "смс ка{n}");
        settings.CommentRules.KaFinal = CleanOrDefault(settings.CommentRules.KaFinal, "смс ка фин");
        settings.TemplateRuleTypes = NormalizeTemplateRuleTypes(settings.TemplateRuleTypes);
        return settings;
    }

    private static async Task SyncTemplatesByRuleTypesAsync(
        AppDbContext db,
        IReadOnlyList<TemplateRuleTypeDto> templateRuleTypes,
        CancellationToken cancellationToken)
    {
        if (templateRuleTypes.Count == 0)
        {
            return;
        }

        var typeById = templateRuleTypes
            .Select(type => new
            {
                Id = NormalizeTemplateRuleTypeId(type.Id),
                Type = type
            })
            .Where(item => !string.IsNullOrWhiteSpace(item.Id))
            .GroupBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().Type, StringComparer.OrdinalIgnoreCase);

        if (typeById.Count == 0)
        {
            return;
        }

        var templates = await db.Templates.ToListAsync(cancellationToken);
        if (templates.Count == 0)
        {
            return;
        }

        var now = DateTime.UtcNow;
        foreach (var template in templates)
        {
            var normalizedKind = NormalizeTemplateRuleTypeId(template.Kind);
            if (string.IsNullOrWhiteSpace(normalizedKind))
            {
                continue;
            }

            if (!typeById.TryGetValue(normalizedKind, out var type))
            {
                continue;
            }

            var manualOnly = !type.AutoAssign;
            var normalizedMode = NormalizeTemplateRuleMode(type.OverdueMode);

            var targetMode = manualOnly ? OverdueModeRange : normalizedMode;
            int? targetFromDays;
            int? targetToDays;
            int? targetExactDay;

            if (manualOnly)
            {
                targetFromDays = 0;
                targetToDays = 0;
                targetExactDay = null;
            }
            else if (normalizedMode == OverdueModeExact)
            {
                targetFromDays = null;
                targetToDays = null;
                targetExactDay = Math.Max(0, type.OverdueExactDay ?? 0);
            }
            else
            {
                var from = Math.Max(0, type.OverdueFromDays ?? 0);
                var to = Math.Max(from, type.OverdueToDays ?? from);
                targetFromDays = from;
                targetToDays = to;
                targetExactDay = null;
            }

            var changed = false;
            if (!string.Equals(template.Kind, normalizedKind, StringComparison.Ordinal))
            {
                template.Kind = normalizedKind;
                changed = true;
            }

            if (!string.Equals(template.OverdueMode, targetMode, StringComparison.OrdinalIgnoreCase))
            {
                template.OverdueMode = targetMode;
                changed = true;
            }

            if (template.OverdueFromDays != targetFromDays)
            {
                template.OverdueFromDays = targetFromDays;
                changed = true;
            }

            if (template.OverdueToDays != targetToDays)
            {
                template.OverdueToDays = targetToDays;
                changed = true;
            }

            if (template.OverdueExactDay != targetExactDay)
            {
                template.OverdueExactDay = targetExactDay;
                changed = true;
            }

            if (template.AutoAssign != type.AutoAssign)
            {
                template.AutoAssign = type.AutoAssign;
                changed = true;
            }

            if (changed)
            {
                template.UpdatedAtUtc = now;
            }
        }
    }

    private static List<TemplateRuleTypeDto> NormalizeTemplateRuleTypes(List<TemplateRuleTypeDto>? items)
    {
        var source = items ?? new List<TemplateRuleTypeDto>();
        var result = new List<TemplateRuleTypeDto>();
        var usedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var fallbackOrder = 10;

        foreach (var item in source.OrderBy(x => x?.SortOrder ?? int.MaxValue))
        {
            if (item is null)
            {
                continue;
            }

            var id = NormalizeTemplateRuleTypeId(item.Id);
            if (string.IsNullOrWhiteSpace(id))
            {
                id = $"type_{result.Count + 1}";
            }

            while (!usedIds.Add(id))
            {
                id = $"{id}_{result.Count + 1}";
            }

            var name = CleanOrDefault(item.Name, id);
            var manualOnly = !item.AutoAssign;
            var mode = manualOnly ? OverdueModeRange : NormalizeTemplateRuleMode(item.OverdueMode);
            var normalized = new TemplateRuleTypeDto
            {
                Id = id,
                Name = name,
                OverdueMode = mode,
                AutoAssign = item.AutoAssign,
                SortOrder = item.SortOrder > 0 ? item.SortOrder : fallbackOrder
            };

            if (manualOnly)
            {
                normalized.OverdueFromDays = 0;
                normalized.OverdueToDays = 0;
                normalized.OverdueExactDay = null;
            }
            else if (mode == OverdueModeExact)
            {
                var exact = item.OverdueExactDay.GetValueOrDefault();
                normalized.OverdueExactDay = Math.Max(0, exact);
                normalized.OverdueFromDays = null;
                normalized.OverdueToDays = null;
            }
            else
            {
                var from = Math.Max(0, item.OverdueFromDays ?? 0);
                var to = Math.Max(from, item.OverdueToDays ?? from);
                normalized.OverdueFromDays = from;
                normalized.OverdueToDays = to;
                normalized.OverdueExactDay = null;
            }

            fallbackOrder += 10;
            result.Add(normalized);
        }

        if (result.Count == 0)
        {
            result.AddRange(DefaultTemplateRuleTypes.Select(x => new TemplateRuleTypeDto
            {
                Id = x.Id,
                Name = x.Name,
                OverdueMode = x.OverdueMode,
                OverdueFromDays = x.OverdueFromDays,
                OverdueToDays = x.OverdueToDays,
                OverdueExactDay = x.OverdueExactDay,
                AutoAssign = x.AutoAssign,
                SortOrder = x.SortOrder
            }));
        }

        return result
            .OrderBy(x => x.SortOrder)
            .ThenBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static string NormalizeTemplateRuleMode(string? value)
    {
        var mode = (value ?? string.Empty).Trim().ToLowerInvariant();
        return mode == OverdueModeExact ? OverdueModeExact : OverdueModeRange;
    }

    private static string NormalizeTemplateRuleTypeId(string? value)
    {
        var raw = (value ?? string.Empty).Trim().ToLowerInvariant();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        var chars = raw
            .Select(ch => char.IsLetterOrDigit(ch) || ch == '_' || ch == '-' ? ch : '_')
            .ToArray();
        var collapsed = new string(chars);
        while (collapsed.Contains("__", StringComparison.Ordinal))
        {
            collapsed = collapsed.Replace("__", "_", StringComparison.Ordinal);
        }

        return collapsed.Trim('_');
    }

    private static string CleanOrDefault(string? value, string fallback)
    {
        var text = (value ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(text) ? fallback : text;
    }

    private static string NormalizeTimeOrDefault(string? value, string fallback)
    {
        var text = (value ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            return fallback;
        }

        if (!TimeOnly.TryParseExact(text, "HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            return fallback;
        }

        return parsed.ToString("HH:mm", CultureInfo.InvariantCulture);
    }
}
