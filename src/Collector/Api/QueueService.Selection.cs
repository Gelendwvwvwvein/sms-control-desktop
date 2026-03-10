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
    private async Task<SelectionResult> SelectRowsAsync(AppDbContext db, QueueFilterRequestDto payload, CancellationToken cancellationToken)
    {
        var snapshot = await ResolveSnapshotAsync(db, payload.SnapshotId, cancellationToken);
        var settings = await settingsStore.GetAsync(db, cancellationToken);

        var parseError = TryParseRanges(payload.OverdueRanges, out var overdueRanges);
        if (parseError is not null)
        {
            throw new InvalidOperationException(parseError.Message);
        }

        var exactOverdueError = TryParseExactOverdueFilter(payload.ExactOverdue, payload.ExactDay, out var exactOverdueRange, out var normalizedExactOverdue);
        if (exactOverdueError is not null)
        {
            throw new InvalidOperationException(exactOverdueError.Message);
        }

        var timezoneOffsets = (payload.TimezoneOffsets ?? [])
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        var snapshotRowsQuery = db.ClientSnapshotRows
            .AsNoTracking()
            .Where(x => x.SnapshotId == snapshot.Id);
        var totalRowsInSnapshot = await snapshotRowsQuery.CountAsync(cancellationToken);

        IQueryable<ClientSnapshotRow> filtered = snapshotRowsQuery;
        if (timezoneOffsets.Count > 0)
        {
            var tzSet = timezoneOffsets.ToHashSet();
            filtered = filtered.Where(x => tzSet.Contains(x.TimezoneOffset));
        }

        if (exactOverdueRange.HasValue)
        {
            var exactOverdue = exactOverdueRange.Value;
            filtered = filtered.Where(x => x.DaysOverdue >= exactOverdue.From && x.DaysOverdue <= exactOverdue.To);
        }
        else if (overdueRanges.Count > 0)
        {
            var overdueDays = overdueRanges
                .SelectMany(r => Enumerable.Range(r.From, (r.To - r.From) + 1))
                .Distinct()
                .ToList();
            filtered = filtered.Where(x => overdueDays.Contains(x.DaysOverdue));
        }

        var matched = await filtered
            .OrderBy(x => x.Id)
            .ToListAsync(cancellationToken);

        var stopPhones = await db.StopList
            .AsNoTracking()
            .Where(x => x.IsActive)
            .Select(x => x.Phone)
            .ToListAsync(cancellationToken);
        var stopSet = stopPhones
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(NormalizePhone)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.Ordinal);

        var excludedByMissingPhone = matched.Count(x => string.IsNullOrWhiteSpace(x.Phone));
        var excludedByStopList = matched.Count(x => !string.IsNullOrWhiteSpace(x.Phone) && stopSet.Contains(NormalizePhone(x.Phone)));

        var readyCandidates = matched
            .Where(x => !string.IsNullOrWhiteSpace(x.Phone))
            .Where(x => !stopSet.Contains(NormalizePhone(x.Phone)))
            .ToList();
        var recentSmsSet = await GetRecentSentPhoneSetAsync(
            db,
            readyCandidates,
            settings.RecentSmsCooldownDays,
            cancellationToken);
        var excludedByRecentSms = readyCandidates.Count(x => recentSmsSet.Contains(NormalizePhone(x.Phone)));
        var readyRows = readyCandidates
            .Where(x => !recentSmsSet.Contains(NormalizePhone(x.Phone)))
            .ToList();

        var appliedFilter = new QueueAppliedFilterDto
        {
            SnapshotId = snapshot.Id,
            TimezoneOffsets = timezoneOffsets,
            OverdueRanges = exactOverdueRange.HasValue
                ? []
                : overdueRanges.Select(r => $"{r.From}-{r.To}").ToList(),
            ExactDay = exactOverdueRange is { From: var from, To: var to } && from == to
                ? from
                : null,
            ExactOverdue = normalizedExactOverdue,
            RecentSmsCooldownDays = settings.RecentSmsCooldownDays
        };

        var preview = new QueuePreviewDto
        {
            SnapshotId = snapshot.Id,
            SourceMode = snapshot.SourceMode,
            TotalRowsInSnapshot = totalRowsInSnapshot,
            MatchedByFilter = matched.Count,
            ExcludedByStopList = excludedByStopList,
            ExcludedByMissingPhone = excludedByMissingPhone,
            ExcludedByRecentSms = excludedByRecentSms,
            ReadyRows = readyRows.Count,
            CanBuild = readyRows.Count > 0,
            AppliedFilter = appliedFilter
        };

        return new SelectionResult
        {
            Snapshot = snapshot,
            ReadyRows = readyRows,
            Preview = preview
        };
    }


    private static ApiErrorDto? TryParseRanges(IEnumerable<string>? rawRanges, out List<OverdueRange> ranges)
    {
        ranges = [];
        var uniqueRanges = (rawRanges ?? [])
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var raw in uniqueRanges)
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                if (day < 0)
                {
                    return new ApiErrorDto
                    {
                        Code = "QUEUE_FILTER_RANGE_INVALID",
                        Message = $"Некорректное значение просрочки: '{raw}'."
                    };
                }

                ranges.Add(new OverdueRange(day, day));
                continue;
            }

            var match = OverdueRangeRegex().Match(raw);
            if (!match.Success)
            {
                return new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_RANGE_INVALID",
                    Message = $"Некорректное значение просрочки: '{raw}'. Ожидается формат 'N' или 'N-M'."
                };
            }

            var from = int.Parse(match.Groups[1].Value);
            var to = int.Parse(match.Groups[2].Value);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_RANGE_INVALID",
                    Message = $"Некорректный диапазон просрочки: '{raw}'."
                };
            }

            ranges.Add(new OverdueRange(from, to));
        }

        ranges = ranges
            .OrderBy(x => x.From)
            .ThenBy(x => x.To)
            .ToList();

        return null;
    }

    private static ApiErrorDto? TryParseExactOverdueFilter(
        string? rawExactOverdue,
        int? legacyExactDay,
        out OverdueRange? range,
        out string normalized)
    {
        range = null;
        normalized = string.Empty;

        var raw = (rawExactOverdue ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(raw))
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                range = new OverdueRange(day, day);
                normalized = day.ToString(CultureInfo.InvariantCulture);
                return null;
            }

            var rangeMatch = OverdueRangeRegex().Match(raw);
            if (!rangeMatch.Success)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"Некорректное значение точной просрочки: '{raw}'. Ожидается формат 'N' или 'N-M'."
                };
            }

            var from = int.Parse(rangeMatch.Groups[1].Value, CultureInfo.InvariantCulture);
            var to = int.Parse(rangeMatch.Groups[2].Value, CultureInfo.InvariantCulture);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"Некорректное значение точной просрочки: '{raw}'."
                };
            }

            range = new OverdueRange(from, to);
            normalized = $"{from.ToString(CultureInfo.InvariantCulture)}-{to.ToString(CultureInfo.InvariantCulture)}";
            return null;
        }

        if (!legacyExactDay.HasValue)
        {
            return null;
        }

        if (legacyExactDay.Value < 0)
        {
            return new ApiErrorDto
            {
                Code = QueueExactOverdueLegacyErrorCode,
                Message = "Точное значение просрочки должно быть >= 0."
            };
        }

        range = new OverdueRange(legacyExactDay.Value, legacyExactDay.Value);
        normalized = legacyExactDay.Value.ToString(CultureInfo.InvariantCulture);
        return null;
    }

    // Kept temporarily for migration safety; new calls use TryParseExactOverdueFilter.
    private static ApiErrorDto? TryParseExactOverdueFilterLegacy(
        string? rawExactOverdue,
        int? legacyExactDay,
        out OverdueRange? range,
        out string normalized)
    {
        range = null;
        normalized = string.Empty;

        var raw = (rawExactOverdue ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(raw))
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                range = new OverdueRange(day, day);
                normalized = day.ToString(CultureInfo.InvariantCulture);
                return null;
            }

            var rangeMatch = OverdueRangeRegex().Match(raw);
            if (!rangeMatch.Success)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ С‚РѕС‡РЅРѕР№ РїСЂРѕСЃСЂРѕС‡РєРё: '{raw}'. РћР¶РёРґР°РµС‚СЃСЏ С„РѕСЂРјР°С‚ 'N' РёР»Рё 'N-M'."
                };
            }

            var from = int.Parse(rangeMatch.Groups[1].Value, CultureInfo.InvariantCulture);
            var to = int.Parse(rangeMatch.Groups[2].Value, CultureInfo.InvariantCulture);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ С‚РѕС‡РЅРѕР№ РїСЂРѕСЃСЂРѕС‡РєРё: '{raw}'."
                };
            }

            range = new OverdueRange(from, to);
            normalized = $"{from.ToString(CultureInfo.InvariantCulture)}-{to.ToString(CultureInfo.InvariantCulture)}";
            return null;
        }

        if (!legacyExactDay.HasValue)
        {
            return null;
        }

        if (legacyExactDay.Value < 0)
        {
            return new ApiErrorDto
            {
                Code = QueueExactOverdueLegacyErrorCode,
                Message = "РўРѕС‡РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ РїСЂРѕСЃСЂРѕС‡РєРё РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ >= 0."
            };
        }

        range = new OverdueRange(legacyExactDay.Value, legacyExactDay.Value);
        normalized = legacyExactDay.Value.ToString(CultureInfo.InvariantCulture);
        return null;
    }

    private static async Task<ClientSnapshot> ResolveSnapshotAsync(AppDbContext db, long? snapshotId, CancellationToken cancellationToken)
    {
        ClientSnapshot? snapshot;
        if (snapshotId.HasValue && snapshotId.Value > 0)
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == snapshotId.Value, cancellationToken);
            if (snapshot is null)
            {
                throw new KeyNotFoundException($"Snapshot с id={snapshotId.Value} не найден.");
            }
        }
        else
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
            if (snapshot is null)
            {
                throw new KeyNotFoundException("Нет актуализированной базы клиентов. Сначала выполните /api/clients/sync.");
            }
        }

        return snapshot;
    }

}
