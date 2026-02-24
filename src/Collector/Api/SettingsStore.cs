using System.Text.Json;
using System.Globalization;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class SettingsStore
{
    private const string AppSettingsKey = "app_settings";
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

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
        settings.WorkWindowStart = NormalizeTimeOrDefault(settings.WorkWindowStart, "08:00");
        settings.WorkWindowEnd = NormalizeTimeOrDefault(settings.WorkWindowEnd, "21:00");

        settings.CommentRules ??= new CommentRulesDto();
        settings.CommentRules.Sms2 = CleanOrDefault(settings.CommentRules.Sms2, "смс2");
        settings.CommentRules.Sms3 = CleanOrDefault(settings.CommentRules.Sms3, "смс3");
        settings.CommentRules.Ka1 = CleanOrDefault(settings.CommentRules.Ka1, "смс от ка");
        settings.CommentRules.KaN = CleanOrDefault(settings.CommentRules.KaN, "смс ка{n}");
        settings.CommentRules.KaFinal = CleanOrDefault(settings.CommentRules.KaFinal, "смс ка фин");
        return settings;
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
