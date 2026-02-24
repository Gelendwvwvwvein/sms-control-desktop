using System.Diagnostics;
using System.Text.RegularExpressions;
using Collector.Config;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Models;
using Collector.Services;
using Microsoft.EntityFrameworkCore;
using Microsoft.Playwright;

namespace Collector.Api;

public sealed class ClientSyncService
{
    private const string SnapshotModeLive = "live";

    public async Task<ClientsSyncResultDto> SyncLiveFromRocketmanAsync(
        AppDbContext db,
        string loginUrl,
        string login,
        string password,
        CancellationToken cancellationToken)
    {
        var selectorConfig = await LoadSelectorConfigAsync(cancellationToken);
        var options = new CollectorOptions
        {
            Headless = true,
            TimeoutMs = 20000,
            Parallelism = 1,
            LoginUrl = loginUrl,
            Debug = false
        };

        var startedAtUtc = DateTime.UtcNow;
        var stopwatch = Stopwatch.StartNew();

        List<ClientRecord> rows;
        using (var playwright = await Playwright.CreateAsync())
        {
            await using var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = true
            });

            var context = await browser.NewContextAsync();
            var page = await context.NewPageAsync();

            var collector = new RocketmanCollector(selectorConfig, options);
            rows = await collector.CollectTableOnlyAsync(page, new Credentials
            {
                Phone = login,
                Password = password
            });
        }

        var snapshot = new ClientSnapshot
        {
            SourceMode = SnapshotModeLive,
            CreatedAtUtc = DateTime.UtcNow,
            TotalRows = rows.Count,
            Notes = "Собрано из таблицы Rocketman без захода в карточки клиентов."
        };

        db.ClientSnapshots.Add(snapshot);
        await db.SaveChangesAsync(cancellationToken);

        var collectedAtUtc = DateTime.UtcNow;
        var mappedRows = rows.Select(row => new ClientSnapshotRow
        {
            SnapshotId = snapshot.Id,
            ExternalClientId = ExtractExternalClientId(row),
            Fio = CleanText(row.Fio),
            Phone = NormalizePhone(row.Phone),
            TimezoneOffset = ParseTimezoneOffset(row.Timezone),
            DaysOverdue = row.DaysOverdue > 0
                ? row.DaysOverdue
                : ParseDaysOverdue(row.DaysOverdueRaw),
            ContractStatus = row.ContractBlueFlag ? "blue" : string.Empty,
            CardUrl = CleanText(row.ClientCardUrl),
            TotalWithCommissionRaw = string.Empty,
            CollectedAtUtc = collectedAtUtc
        }).ToList();

        if (mappedRows.Count > 0)
        {
            db.ClientSnapshotRows.AddRange(mappedRows);
            await db.SaveChangesAsync(cancellationToken);
        }

        stopwatch.Stop();
        var finishedAtUtc = DateTime.UtcNow;
        return new ClientsSyncResultDto
        {
            SnapshotId = snapshot.Id,
            SourceMode = snapshot.SourceMode,
            TotalRows = snapshot.TotalRows,
            StartedAtUtc = startedAtUtc,
            FinishedAtUtc = finishedAtUtc,
            DurationMs = stopwatch.ElapsedMilliseconds,
            CardTotalsSkipped = true
        };
    }

    public async Task<ClientsSyncStatusDto> GetLatestSyncStatusAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var latest = await db.ClientSnapshots
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        if (latest is null)
        {
            return new ClientsSyncStatusDto { HasSnapshot = false };
        }

        return new ClientsSyncStatusDto
        {
            HasSnapshot = true,
            SnapshotId = latest.Id,
            SourceMode = latest.SourceMode,
            CreatedAtUtc = latest.CreatedAtUtc,
            TotalRows = latest.TotalRows,
            Notes = latest.Notes ?? string.Empty
        };
    }

    public async Task<ClientsListDto> ListClientsAsync(
        AppDbContext db,
        long? snapshotId,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        ClientSnapshot? snapshot;
        if (snapshotId.HasValue && snapshotId.Value > 0)
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == snapshotId.Value, cancellationToken);
        }
        else
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
        }

        if (snapshot is null)
        {
            return new ClientsListDto
            {
                Snapshot = new ClientsSyncStatusDto { HasSnapshot = false },
                TotalRowsInSnapshot = 0,
                Items = []
            };
        }

        var totalRowsInSnapshot = await db.ClientSnapshotRows
            .AsNoTracking()
            .CountAsync(x => x.SnapshotId == snapshot.Id, cancellationToken);

        var rows = await db.ClientSnapshotRows
            .AsNoTracking()
            .Where(x => x.SnapshotId == snapshot.Id)
            .OrderBy(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        var phones = rows
            .Select(x => NormalizePhone(x.Phone))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var externalClientIds = rows
            .Select(x => x.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        HashSet<string> dialogPhonesWithHistory = [];
        if (phones.Count > 0)
        {
            dialogPhonesWithHistory = (await db.Messages
                    .AsNoTracking()
                    .Where(x => phones.Contains(x.ClientPhone))
                    .Select(x => x.ClientPhone)
                    .Distinct()
                    .ToListAsync(cancellationToken))
                .Select(NormalizePhone)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .ToHashSet(StringComparer.Ordinal);
        }

        var latestRunSessionId = await db.RunSessions
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .Select(x => (long?)x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        HashSet<string> plannedExternalClientIds = [];
        if (latestRunSessionId.HasValue && latestRunSessionId.Value > 0 && externalClientIds.Count > 0)
        {
            plannedExternalClientIds = (await db.RunJobs
                    .AsNoTracking()
                    .Where(x => x.RunSessionId == latestRunSessionId.Value)
                    .Where(x => externalClientIds.Contains(x.ExternalClientId))
                    .Select(x => x.ExternalClientId)
                    .Distinct()
                    .ToListAsync(cancellationToken))
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .ToHashSet(StringComparer.Ordinal);
        }

        var debtCacheByExternalClientId = externalClientIds.Count == 0
            ? new Dictionary<string, ClientDebtCacheRecord>(StringComparer.Ordinal)
            : await db.ClientDebtCache
                .AsNoTracking()
                .Where(x => externalClientIds.Contains(x.ExternalClientId))
                .ToDictionaryAsync(x => x.ExternalClientId, cancellationToken);

        return new ClientsListDto
        {
            Snapshot = new ClientsSyncStatusDto
            {
                HasSnapshot = true,
                SnapshotId = snapshot.Id,
                SourceMode = snapshot.SourceMode,
                CreatedAtUtc = snapshot.CreatedAtUtc,
                TotalRows = snapshot.TotalRows,
                Notes = snapshot.Notes ?? string.Empty
            },
            TotalRowsInSnapshot = totalRowsInSnapshot,
            Items = rows.Select(row =>
                    MapRowToDto(
                        row,
                        debtCacheByExternalClientId.TryGetValue(row.ExternalClientId, out var debtCache) ? debtCache : null,
                        dialogPhonesWithHistory.Contains(NormalizePhone(row.Phone)),
                        plannedExternalClientIds.Contains(row.ExternalClientId),
                        latestRunSessionId))
                .ToList()
        };
    }

    public static ApiErrorDto? ValidateSyncCredentials(string loginUrl, string login, string password)
    {
        if (string.IsNullOrWhiteSpace(loginUrl))
        {
            return new ApiErrorDto
            {
                Code = "SYNC_LOGIN_URL_REQUIRED",
                Message = "Для актуализации базы заполните URL входа в настройках."
            };
        }

        if (string.IsNullOrWhiteSpace(login))
        {
            return new ApiErrorDto
            {
                Code = "SYNC_LOGIN_REQUIRED",
                Message = "Для актуализации базы заполните логин в настройках."
            };
        }

        if (string.IsNullOrWhiteSpace(password))
        {
            return new ApiErrorDto
            {
                Code = "SYNC_PASSWORD_REQUIRED",
                Message = "Для актуализации базы заполните пароль в настройках."
            };
        }

        return null;
    }

    private static ClientListItemDto MapRowToDto(
        ClientSnapshotRow row,
        ClientDebtCacheRecord? debtCache,
        bool hasDialogHistory,
        bool inPlan,
        long? inPlanRunSessionId)
    {
        var exactTotalRaw = FirstNonEmpty(debtCache?.ExactTotalRaw, row.TotalWithCommissionRaw);
        var approxDebtText = FirstNonEmpty(debtCache?.ApproxTotalText, BuildApproxDebtText(exactTotalRaw));

        return new ClientListItemDto
        {
            Id = row.Id,
            SnapshotId = row.SnapshotId,
            ExternalClientId = row.ExternalClientId,
            Fio = row.Fio,
            Phone = row.Phone,
            TimezoneOffset = row.TimezoneOffset,
            DaysOverdue = row.DaysOverdue,
            ContractBlueFlag = string.Equals(row.ContractStatus, "blue", StringComparison.OrdinalIgnoreCase),
            CardUrl = row.CardUrl ?? string.Empty,
            TotalWithCommissionRaw = exactTotalRaw,
            DebtApproxText = approxDebtText,
            DebtApproxValue = debtCache?.ApproxTotalValue,
            DebtStatus = FirstNonEmpty(debtCache?.Status, string.IsNullOrWhiteSpace(exactTotalRaw) ? "empty" : "ready"),
            DebtSource = FirstNonEmpty(debtCache?.Source, string.IsNullOrWhiteSpace(exactTotalRaw) ? string.Empty : "snapshot"),
            DebtUpdatedAtUtc = debtCache?.UpdatedAtUtc,
            DebtErrorCode = debtCache?.LastErrorCode ?? string.Empty,
            DebtErrorDetail = debtCache?.LastErrorDetail ?? string.Empty,
            CollectedAtUtc = row.CollectedAtUtc,
            DialogStatus = hasDialogHistory ? "has_history" : "none",
            InPlan = inPlan,
            InPlanRunSessionId = inPlan ? inPlanRunSessionId : null
        };
    }

    private static string BuildApproxDebtText(string exactTotalRaw)
    {
        if (string.IsNullOrWhiteSpace(exactTotalRaw))
        {
            return string.Empty;
        }

        var payload = System.Text.Json.JsonSerializer.Serialize(new
        {
            totalWithCommissionRaw = exactTotalRaw
        });
        return RuleEngineService.BuildApproxDebtText(payload);
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

    private static string ResolveSelectorsPath()
    {
        var candidates = new[]
        {
            Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "src/Collector/Config/rocketman.selectors.json")),
            Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), "Config/rocketman.selectors.json")),
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "Config/rocketman.selectors.json")),
            Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "Config/rocketman.selectors.json"))
        };

        foreach (var candidate in candidates)
        {
            if (File.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new FileNotFoundException("Не найден файл селекторов Rocketman.", "rocketman.selectors.json");
    }

    private static async Task<SelectorConfig> LoadSelectorConfigAsync(CancellationToken cancellationToken)
    {
        var path = ResolveSelectorsPath();
        var json = await File.ReadAllTextAsync(path, cancellationToken);
        var config = System.Text.Json.JsonSerializer.Deserialize<SelectorConfig>(json);
        if (config is null)
        {
            throw new InvalidOperationException("Не удалось прочитать конфиг селекторов Rocketman.");
        }

        return config;
    }

    private static string NormalizePhone(string rawPhone)
    {
        var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 0) return string.Empty;
        return $"+{digits}";
    }

    private static int ParseTimezoneOffset(string timezoneRaw)
    {
        if (string.IsNullOrWhiteSpace(timezoneRaw)) return 0;

        var input = timezoneRaw.Trim();
        var moscowMatch = Regex.Match(input, @"[Мм][Сс][Кк]\s*([+-]\d{1,2})");
        if (moscowMatch.Success && int.TryParse(moscowMatch.Groups[1].Value, out var mskOffset))
        {
            return mskOffset;
        }

        var simpleMatch = Regex.Match(input, @"([+-]\d{1,2})");
        if (simpleMatch.Success && int.TryParse(simpleMatch.Groups[1].Value, out var offset))
        {
            return offset;
        }

        return 0;
    }

    private static int ParseDaysOverdue(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return 0;
        }

        var input = raw.Replace('\u00A0', ' ').Trim();

        var slashPattern = Regex.Match(input, @"(?<!\d)(\d{1,3})\s*/\s*\d{1,3}(?!\d)");
        if (slashPattern.Success && int.TryParse(slashPattern.Groups[1].Value, out var slashValue))
        {
            return slashValue;
        }

        var overduePattern = Regex.Match(input, @"[Пп]росроч\w*\D{0,20}(\d{1,3})");
        if (overduePattern.Success && int.TryParse(overduePattern.Groups[1].Value, out var overdueValue))
        {
            return overdueValue;
        }

        var plainPattern = Regex.Match(input, @"(?<!\d)(\d{1,3})(?!\d)");
        if (plainPattern.Success && int.TryParse(plainPattern.Groups[1].Value, out var plainValue) && plainValue <= 180)
        {
            return plainValue;
        }

        return 0;
    }

    private static string ExtractExternalClientId(ClientRecord row)
    {
        var byUrl = TryExtractIdFromUrl(row.ClientCardUrl);
        if (!string.IsNullOrWhiteSpace(byUrl))
        {
            return byUrl;
        }

        var byKey = TryExtractIdFromUrl(row.ContractKey);
        if (!string.IsNullOrWhiteSpace(byKey))
        {
            return byKey;
        }

        var phone = NormalizePhone(row.Phone);
        if (!string.IsNullOrWhiteSpace(phone))
        {
            return $"phone:{phone}";
        }

        var fio = CleanText(row.Fio);
        return string.IsNullOrWhiteSpace(fio) ? $"row:{Guid.NewGuid():N}" : $"fio:{fio}";
    }

    private static string TryExtractIdFromUrl(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return string.Empty;
        var input = raw.Trim();

        if (!input.StartsWith("http", StringComparison.OrdinalIgnoreCase) &&
            !input.StartsWith("file", StringComparison.OrdinalIgnoreCase))
        {
            input = $"https://rocketman.ru{(input.StartsWith('/') ? string.Empty : "/")}{input}";
        }

        if (Uri.TryCreate(input, UriKind.Absolute, out var uri))
        {
            var query = uri.Query.TrimStart('?').Split('&', StringSplitOptions.RemoveEmptyEntries);
            foreach (var pair in query)
            {
                var kv = pair.Split('=', 2);
                if (kv.Length == 2 && kv[0].Equals("id", StringComparison.OrdinalIgnoreCase))
                {
                    var idValue = Uri.UnescapeDataString(kv[1]);
                    if (idValue.All(char.IsDigit)) return idValue;
                }
            }
        }

        var regex = Regex.Match(raw, @"[?&]id=(\d+)");
        return regex.Success ? regex.Groups[1].Value : string.Empty;
    }

    private static string CleanText(string? value)
    {
        return (value ?? string.Empty).Trim();
    }
}
