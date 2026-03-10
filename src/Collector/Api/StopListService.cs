using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class StopListService
{
    public async Task<List<StopListDto>> ListAsync(AppDbContext db, bool activeOnly, CancellationToken cancellationToken)
    {
        var query = db.StopList.AsNoTracking();
        if (activeOnly)
        {
            query = query.Where(x => x.IsActive);
        }

        var records = await query
            .OrderByDescending(x => x.IsActive)
            .ThenByDescending(x => x.AddedAtUtc)
            .ThenByDescending(x => x.Id)
            .ToListAsync(cancellationToken);

        return records.Select(MapToDto).ToList();
    }

    public async Task<StopListDto?> GetByIdAsync(AppDbContext db, long id, CancellationToken cancellationToken)
    {
        var record = await db.StopList
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == id, cancellationToken);

        return record is null ? null : MapToDto(record);
    }

    public async Task<List<StopListDto>> ListByPhoneAsync(AppDbContext db, string phone, bool activeOnly, CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        var query = db.StopList
            .AsNoTracking()
            .Where(x => x.Phone == normalizedPhone);

        if (activeOnly)
        {
            query = query.Where(x => x.IsActive);
        }

        var records = await query
            .OrderByDescending(x => x.AddedAtUtc)
            .ThenByDescending(x => x.Id)
            .ToListAsync(cancellationToken);

        return records.Select(MapToDto).ToList();
    }

    public async Task<StopListDto> CreateOrActivateAsync(AppDbContext db, StopListUpsertRequest payload, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        var normalizedPhone = NormalizePhone(payload.Phone);
        var normalizedReason = NormalizeReason(payload.Reason);
        var normalizedSource = NormalizeSource(payload.Source);

        var record = await db.StopList
            .OrderByDescending(x => x.Id)
            .FirstOrDefaultAsync(x => x.Phone == normalizedPhone, cancellationToken);

        if (record is null)
        {
            record = new StopListRecord
            {
                Phone = normalizedPhone,
                Reason = normalizedReason,
                Source = normalizedSource,
                AddedAtUtc = now,
                IsActive = payload.IsActive
            };

            db.StopList.Add(record);
        }
        else
        {
            record.Reason = normalizedReason;
            record.Source = normalizedSource;
            record.IsActive = payload.IsActive;

            if (record.IsActive)
            {
                record.AddedAtUtc = now;
            }

            db.StopList.Update(record);
        }

        await db.SaveChangesAsync(cancellationToken);
        return MapToDto(record);
    }

    public async Task<StopListDto?> UpdateAsync(AppDbContext db, long id, StopListUpsertRequest payload, CancellationToken cancellationToken)
    {
        var record = await db.StopList.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return null;

        record.Phone = NormalizePhone(payload.Phone);
        record.Reason = NormalizeReason(payload.Reason);
        record.Source = NormalizeSource(payload.Source);
        record.IsActive = payload.IsActive;

        if (record.IsActive)
        {
            record.AddedAtUtc = DateTime.UtcNow;
        }

        db.StopList.Update(record);
        await db.SaveChangesAsync(cancellationToken);
        return MapToDto(record);
    }

    public async Task<bool> DeactivateByIdAsync(AppDbContext db, long id, CancellationToken cancellationToken)
    {
        var record = await db.StopList.FirstOrDefaultAsync(x => x.Id == id, cancellationToken);
        if (record is null) return false;

        if (record.IsActive)
        {
            record.IsActive = false;
            db.StopList.Update(record);
            await db.SaveChangesAsync(cancellationToken);
        }

        return true;
    }

    public async Task<int> DeactivateByPhoneAsync(AppDbContext db, string phone, CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        var activeRows = await db.StopList
            .Where(x => x.Phone == normalizedPhone && x.IsActive)
            .ToListAsync(cancellationToken);

        if (activeRows.Count == 0) return 0;

        foreach (var row in activeRows)
        {
            row.IsActive = false;
        }

        db.StopList.UpdateRange(activeRows);
        await db.SaveChangesAsync(cancellationToken);
        return activeRows.Count;
    }

    public async Task<StopListBulkResultDto> BulkAddAsync(
        AppDbContext db,
        StopListBulkAddRequest payload,
        CancellationToken cancellationToken)
    {
        var reason = NormalizeReason(payload.Reason) ?? "Добавлено массово";
        var source = NormalizeSource(payload.Source);

        var validPhones = new List<string>();
        var invalidPhones = new List<string>();
        foreach (var phone in (payload.Phones ?? []).Distinct())
        {
            if (TryNormalizePhone(phone, out var normalized) && !string.IsNullOrEmpty(normalized))
            {
                validPhones.Add(normalized);
            }
            else if (!string.IsNullOrWhiteSpace(phone))
            {
                invalidPhones.Add(phone.Trim());
            }
        }

        if (validPhones.Count == 0)
        {
            return new StopListBulkResultDto
            {
                Requested = (payload.Phones ?? []).Count,
                Added = 0,
                Removed = 0,
                Skipped = 0,
                InvalidPhones = invalidPhones
            };
        }

        var now = DateTime.UtcNow;
        var existingRecords = await db.StopList
            .Where(x => validPhones.Contains(x.Phone))
            .OrderByDescending(x => x.Id)
            .ToListAsync(cancellationToken);
        var latestByPhone = existingRecords
            .GroupBy(x => x.Phone)
            .ToDictionary(x => x.Key, x => x.First(), StringComparer.Ordinal);

        foreach (var normalizedPhone in validPhones)
        {
            if (latestByPhone.TryGetValue(normalizedPhone, out var record))
            {
                record.Reason = reason;
                record.Source = source;
                record.IsActive = true;
                record.AddedAtUtc = now;
                db.StopList.Update(record);
                continue;
            }

            db.StopList.Add(new StopListRecord
            {
                Phone = normalizedPhone,
                Reason = reason,
                Source = source,
                AddedAtUtc = now,
                IsActive = true
            });
        }

        await db.SaveChangesAsync(cancellationToken);

        return new StopListBulkResultDto
        {
            Requested = (payload.Phones ?? []).Count,
            Added = validPhones.Count,
            Removed = 0,
            Skipped = 0,
            InvalidPhones = invalidPhones
        };
    }

    public async Task<StopListBulkResultDto> BulkRemoveByPhonesAsync(
        AppDbContext db,
        StopListBulkRemoveRequest payload,
        CancellationToken cancellationToken)
    {
        var validPhones = new List<string>();
        var invalidPhones = new List<string>();
        foreach (var phone in (payload.Phones ?? []).Distinct())
        {
            if (TryNormalizePhone(phone, out var normalized) && !string.IsNullOrEmpty(normalized))
            {
                validPhones.Add(normalized);
            }
            else if (!string.IsNullOrWhiteSpace(phone))
            {
                invalidPhones.Add(phone.Trim());
            }
        }

        if (validPhones.Count == 0)
        {
            return new StopListBulkResultDto
            {
                Requested = (payload.Phones ?? []).Count,
                Added = 0,
                Removed = 0,
                Skipped = 0,
                InvalidPhones = invalidPhones
            };
        }

        var activeRows = await db.StopList
            .Where(x => validPhones.Contains(x.Phone) && x.IsActive)
            .ToListAsync(cancellationToken);

        foreach (var row in activeRows)
        {
            row.IsActive = false;
        }

        if (activeRows.Count > 0)
        {
            db.StopList.UpdateRange(activeRows);
            await db.SaveChangesAsync(cancellationToken);
        }

        var removedPhones = activeRows
            .Select(x => x.Phone)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .Count();

        return new StopListBulkResultDto
        {
            Requested = (payload.Phones ?? []).Count,
            Added = 0,
            Removed = removedPhones,
            Skipped = Math.Max(0, validPhones.Count - removedPhones),
            InvalidPhones = invalidPhones
        };
    }

    public async Task<int> BulkDeactivateByIdsAsync(
        AppDbContext db,
        StopListBulkDeactivateRequest payload,
        CancellationToken cancellationToken)
    {
        var ids = (payload.Ids ?? [])
            .Where(x => x > 0)
            .Distinct()
            .ToList();
        if (ids.Count == 0) return 0;

        var records = await db.StopList
            .Where(x => ids.Contains(x.Id) && x.IsActive)
            .ToListAsync(cancellationToken);
        if (records.Count == 0) return 0;

        foreach (var r in records)
        {
            r.IsActive = false;
        }
        db.StopList.UpdateRange(records);
        await db.SaveChangesAsync(cancellationToken);
        return records.Count;
    }

    public static ApiErrorDto? ValidateUpsertRequest(StopListUpsertRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (!TryNormalizePhone(payload.Phone, out _))
        {
            return new ApiErrorDto
            {
                Code = "STOP_LIST_PHONE_INVALID",
                Message = "Телефон обязателен и должен содержать от 10 до 15 цифр."
            };
        }

        if (!string.IsNullOrWhiteSpace(payload.Reason) && payload.Reason.Trim().Length > 512)
        {
            return new ApiErrorDto
            {
                Code = "STOP_LIST_REASON_TOO_LONG",
                Message = "Причина не должна превышать 512 символов."
            };
        }

        var rawSource = (payload.Source ?? string.Empty).Trim();
        if (rawSource.Length > 64)
        {
            return new ApiErrorDto
            {
                Code = "STOP_LIST_SOURCE_INVALID",
                Message = "Источник должен содержать от 1 до 64 символов."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidatePhone(string? rawPhone)
    {
        if (!TryNormalizePhone(rawPhone, out _))
        {
            return new ApiErrorDto
            {
                Code = "STOP_LIST_PHONE_INVALID",
                Message = "Телефон обязателен и должен содержать от 10 до 15 цифр."
            };
        }

        return null;
    }

    private static string NormalizePhone(string rawPhone)
    {
        if (!TryNormalizePhone(rawPhone, out var normalized))
        {
            return string.Empty;
        }

        return normalized;
    }

    private static bool TryNormalizePhone(string? rawPhone, out string normalized)
    {
        normalized = PhoneNormalizer.Normalize(rawPhone, minDigits: 10, maxDigits: 15);
        return !string.IsNullOrEmpty(normalized);
    }

    private static string NormalizeSource(string? rawSource)
    {
        var source = (rawSource ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(source))
        {
            return "manual";
        }

        return source;
    }

    private static string? NormalizeReason(string? reason)
    {
        var normalized = (reason ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(normalized) ? null : normalized;
    }

    private static StopListDto MapToDto(StopListRecord record)
    {
        return new StopListDto
        {
            Id = record.Id,
            Phone = record.Phone,
            Reason = record.Reason ?? string.Empty,
            Source = record.Source,
            AddedAtUtc = record.AddedAtUtc,
            IsActive = record.IsActive
        };
    }
}
