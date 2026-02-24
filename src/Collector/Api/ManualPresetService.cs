using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class ManualPresetService
{
    private static readonly HashSet<string> AllowedTokens = new(StringComparer.Ordinal)
    {
        "{полное_фио}",
        "{сумма_долга}"
    };

    public async Task<List<ManualPresetDto>> ListAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var records = await db.ManualReplyPresets
            .AsNoTracking()
            .OrderByDescending(x => x.UpdatedAtUtc)
            .ThenByDescending(x => x.Id)
            .ToListAsync(cancellationToken);

        return records.Select(MapToDto).ToList();
    }

    public async Task<ManualPresetDto?> GetByIdAsync(AppDbContext db, long id, CancellationToken cancellationToken)
    {
        var record = await db.ManualReplyPresets
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);

        return record is null ? null : MapToDto(record);
    }

    public async Task<ManualPresetDto> CreateAsync(AppDbContext db, ManualPresetUpsertRequest payload, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var record = new ManualReplyPresetRecord
        {
            Title = payload.Title.Trim(),
            Text = payload.Text.Trim(),
            CreatedAtUtc = now,
            UpdatedAtUtc = now
        };

        db.ManualReplyPresets.Add(record);
        await db.SaveChangesAsync(cancellationToken);
        return MapToDto(record);
    }

    public async Task<ManualPresetDto?> UpdateAsync(AppDbContext db, long id, ManualPresetUpsertRequest payload, CancellationToken cancellationToken)
    {
        var record = await db.ManualReplyPresets.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return null;

        record.Title = payload.Title.Trim();
        record.Text = payload.Text.Trim();
        record.UpdatedAtUtc = DateTime.UtcNow;

        db.ManualReplyPresets.Update(record);
        await db.SaveChangesAsync(cancellationToken);
        return MapToDto(record);
    }

    public async Task<bool> DeleteAsync(AppDbContext db, long id, CancellationToken cancellationToken)
    {
        var record = await db.ManualReplyPresets.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return false;

        db.ManualReplyPresets.Remove(record);
        await db.SaveChangesAsync(cancellationToken);
        return true;
    }

    public static ApiErrorDto? ValidateUpsertRequest(ManualPresetUpsertRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Title))
        {
            return new ApiErrorDto { Code = "MANUAL_PRESET_TITLE_REQUIRED", Message = "Название типового ответа обязательно." };
        }

        if (payload.Title.Trim().Length > 256)
        {
            return new ApiErrorDto
            {
                Code = "MANUAL_PRESET_TITLE_TOO_LONG",
                Message = "Название типового ответа не должно превышать 256 символов."
            };
        }

        if (string.IsNullOrWhiteSpace(payload.Text))
        {
            return new ApiErrorDto { Code = "MANUAL_PRESET_TEXT_REQUIRED", Message = "Текст типового ответа обязателен." };
        }

        var tokenValidationError = ValidateTextTokens(payload.Text);
        if (tokenValidationError is not null)
        {
            return tokenValidationError;
        }

        return null;
    }

    private static ApiErrorDto? ValidateTextTokens(string text)
    {
        var matches = PresetTokenRegex().Matches(text);
        var tokens = matches
            .Select(x => x.Value.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var token in tokens)
        {
            if (!AllowedTokens.Contains(token))
            {
                return new ApiErrorDto
                {
                    Code = "MANUAL_PRESET_TOKEN_UNSUPPORTED",
                    Message = $"Недопустимая переменная в типовом ответе: {token}. Разрешены только {{полное_фио}} и {{сумма_долга}}."
                };
            }
        }

        return null;
    }

    private static ManualPresetDto MapToDto(ManualReplyPresetRecord record)
    {
        return new ManualPresetDto
        {
            Id = record.Id,
            Title = record.Title,
            Text = record.Text,
            CreatedAtUtc = record.CreatedAtUtc,
            UpdatedAtUtc = record.UpdatedAtUtc
        };
    }

    [GeneratedRegex(@"\{[^{}]+\}", RegexOptions.CultureInvariant)]
    private static partial Regex PresetTokenRegex();
}
