using System.Text.Json;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;
using System.Globalization;

namespace Collector.Api;

public sealed class DialogService(SettingsStore settingsStore, AlertService alertService)
{
    private const string ChannelStatusOnline = "online";
    private const string ChannelStatusError = "error";
    private const string ChannelStatusOffline = "offline";

    private readonly TraccarHttpSmsSender _traccarSender = new(new HttpClient());

    public async Task<DialogListDto> ListAsync(
        AppDbContext db,
        int limit,
        int offset,
        string? search,
        CancellationToken cancellationToken)
    {
        var searchNormalized = (search ?? string.Empty).Trim();
        var latestSnapshotId = await db.ClientSnapshots
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .Select(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        IQueryable<MessageRecord> baseQuery = db.Messages.AsNoTracking();
        if (!string.IsNullOrWhiteSpace(searchNormalized))
        {
            var searchPhone = NormalizePhone(searchNormalized);
            var snapshotMatchedPhones = new List<string>();
            if (latestSnapshotId > 0)
            {
                var snapshotPhoneRows = await db.ClientSnapshotRows
                    .AsNoTracking()
                    .Where(x =>
                        x.SnapshotId == latestSnapshotId &&
                        (x.Fio.Contains(searchNormalized) ||
                         x.ExternalClientId.Contains(searchNormalized) ||
                         x.Phone.Contains(searchNormalized) ||
                         (!string.IsNullOrWhiteSpace(searchPhone) && x.Phone.Contains(searchPhone))))
                    .Select(x => x.Phone)
                    .ToListAsync(cancellationToken);

                snapshotMatchedPhones = snapshotPhoneRows
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Select(NormalizePhone)
                    .Where(x => !string.IsNullOrWhiteSpace(x))
                    .Distinct(StringComparer.Ordinal)
                    .ToList();
            }

            baseQuery = baseQuery.Where(x =>
                x.ClientPhone.Contains(searchNormalized) ||
                x.Text.Contains(searchNormalized) ||
                (!string.IsNullOrWhiteSpace(searchPhone) && x.ClientPhone.Contains(searchPhone)) ||
                snapshotMatchedPhones.Contains(x.ClientPhone));
        }

        var groupedQuery = baseQuery
            .GroupBy(x => x.ClientPhone)
            .Select(g => new
            {
                Phone = g.Key,
                LastMessageAtUtc = g.Max(x => x.CreatedAtUtc),
                TotalMessages = g.Count(),
                HasIncoming = g.Any(x => x.Direction == "in")
            });

        var totalDialogs = await groupedQuery.CountAsync(cancellationToken);
        var page = await groupedQuery
            .OrderByDescending(x => x.LastMessageAtUtc)
            .ThenByDescending(x => x.Phone)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        if (page.Count == 0)
        {
            return new DialogListDto
            {
                TotalDialogs = totalDialogs,
                Items = []
            };
        }

        var phones = page.Select(x => x.Phone).ToList();

        var latestMessages = await db.Messages
            .AsNoTracking()
            .Where(x => phones.Contains(x.ClientPhone))
            .OrderByDescending(x => x.CreatedAtUtc)
            .ThenByDescending(x => x.Id)
            .ToListAsync(cancellationToken);

        var latestByPhone = latestMessages
            .GroupBy(x => x.ClientPhone)
            .ToDictionary(x => x.Key, x => x.First());
        var latestOutgoingByPhone = latestMessages
            .Where(x => string.Equals(x.Direction, "out", StringComparison.OrdinalIgnoreCase))
            .GroupBy(x => x.ClientPhone)
            .ToDictionary(x => x.Key, x => x.First());

        var fioByPhone = new Dictionary<string, string>(StringComparer.Ordinal);
        var contractNumberByPhone = new Dictionary<string, string>(StringComparer.Ordinal);
        if (latestSnapshotId > 0)
        {
            var snapshotRows = await db.ClientSnapshotRows
                .AsNoTracking()
                .Where(x => x.SnapshotId == latestSnapshotId && phones.Contains(x.Phone))
                .Select(x => new { x.Phone, x.Fio, x.ExternalClientId, x.CollectedAtUtc, x.Id })
                .ToListAsync(cancellationToken);

            fioByPhone = snapshotRows
                .GroupBy(x => x.Phone)
                .ToDictionary(
                    x => x.Key,
                    x => x
                        .OrderByDescending(v => v.CollectedAtUtc)
                        .ThenByDescending(v => v.Id)
                        .Select(v => v.Fio ?? string.Empty)
                        .FirstOrDefault() ?? string.Empty,
                    StringComparer.Ordinal);

            contractNumberByPhone = snapshotRows
                .GroupBy(x => x.Phone)
                .ToDictionary(
                    x => x.Key,
                    x => x
                        .OrderByDescending(v => v.CollectedAtUtc)
                        .ThenByDescending(v => v.Id)
                        .Select(v => v.ExternalClientId ?? string.Empty)
                        .FirstOrDefault() ?? string.Empty,
                    StringComparer.Ordinal);
        }

        var channelMetaByPhone = new Dictionary<string, (long ChannelId, string ChannelName)>(StringComparer.Ordinal);
        var channelIdSet = new HashSet<long>();
        foreach (var pair in latestOutgoingByPhone)
        {
            TryReadChannelFromMeta(pair.Value.MetaJson, out var channelId, out var channelName);
            if (channelId > 0)
            {
                channelIdSet.Add(channelId);
            }

            channelMetaByPhone[pair.Key] = (channelId, channelName);
        }

        var bindingsByPhone = await db.ClientChannelBindings
            .AsNoTracking()
            .Where(x => phones.Contains(x.Phone))
            .OrderByDescending(x => x.UpdatedAtUtc)
            .ThenByDescending(x => x.Id)
            .ToListAsync(cancellationToken);
        var bindingChannelByPhone = bindingsByPhone
            .GroupBy(x => x.Phone)
            .ToDictionary(x => x.Key, x => x.First().ChannelId, StringComparer.Ordinal);
        foreach (var binding in bindingChannelByPhone.Values)
        {
            if (binding > 0)
            {
                channelIdSet.Add(binding);
            }
        }

        var channelNameById = new Dictionary<long, string>();
        if (channelIdSet.Count > 0)
        {
            var channelIds = channelIdSet.ToList();
            channelNameById = await db.SenderChannels
                .AsNoTracking()
                .Where(x => channelIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x.Name ?? string.Empty, cancellationToken);
        }

        var items = page.Select(x =>
        {
            latestByPhone.TryGetValue(x.Phone, out var last);
            fioByPhone.TryGetValue(x.Phone, out var fio);
            contractNumberByPhone.TryGetValue(x.Phone, out var contractNumber);
            channelMetaByPhone.TryGetValue(x.Phone, out var channelMeta);
            var channelId = channelMeta.ChannelId;
            var channelName = channelMeta.ChannelName;
            if (channelId <= 0 && bindingChannelByPhone.TryGetValue(x.Phone, out var boundChannelId))
            {
                channelId = boundChannelId;
            }

            if (string.IsNullOrWhiteSpace(channelName) && channelId > 0 && channelNameById.TryGetValue(channelId, out var resolvedChannelName))
            {
                channelName = resolvedChannelName;
            }

            return new DialogSummaryDto
            {
                DialogId = BuildDialogId(x.Phone),
                Phone = x.Phone,
                Fio = fio ?? string.Empty,
                ContractNumber = contractNumber ?? string.Empty,
                LastMessageAtUtc = x.LastMessageAtUtc,
                LastDirection = last?.Direction ?? string.Empty,
                LastText = last?.Text ?? string.Empty,
                TotalMessages = x.TotalMessages,
                HasIncoming = x.HasIncoming,
                LastOutgoingChannelId = channelId,
                LastOutgoingChannelName = channelName ?? string.Empty
            };
        }).ToList();

        return new DialogListDto
        {
            TotalDialogs = totalDialogs,
            Items = items
        };
    }

    public async Task<DialogMessagesDto> GetMessagesByPhoneAsync(
        AppDbContext db,
        string phone,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogMessagesDto
            {
                DialogId = string.Empty,
                Phone = string.Empty,
                TotalMessages = 0,
                Items = []
            };
        }

        var totalMessages = await db.Messages
            .AsNoTracking()
            .CountAsync(x => x.ClientPhone == normalizedPhone, cancellationToken);

        var rows = await db.Messages
            .AsNoTracking()
            .Where(x => x.ClientPhone == normalizedPhone)
            .OrderBy(x => x.CreatedAtUtc)
            .ThenBy(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        return new DialogMessagesDto
        {
            DialogId = BuildDialogId(normalizedPhone),
            Phone = normalizedPhone,
            TotalMessages = totalMessages,
            Items = rows.Select(MapMessage).ToList()
        };
    }

    public async Task<DialogMessagesDto?> GetMessagesByClientExternalIdAsync(
        AppDbContext db,
        string externalClientId,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var normalizedClientId = (externalClientId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedClientId))
        {
            return null;
        }

        var latestSnapshotId = await db.ClientSnapshots
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .Select(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        if (latestSnapshotId <= 0)
        {
            return null;
        }

        var phone = await db.ClientSnapshotRows
            .AsNoTracking()
            .Where(x => x.SnapshotId == latestSnapshotId && x.ExternalClientId == normalizedClientId)
            .OrderByDescending(x => x.CollectedAtUtc)
            .ThenByDescending(x => x.Id)
            .Select(x => x.Phone)
            .FirstOrDefaultAsync(cancellationToken);

        if (string.IsNullOrWhiteSpace(phone))
        {
            return null;
        }

        return await GetMessagesByPhoneAsync(db, phone, limit, offset, cancellationToken);
    }

    public async Task<DialogManualSendResultDto> SendManualAsync(
        AppDbContext db,
        string phone,
        DialogManualSendRequest payload,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        var text = (payload.Text ?? string.Empty).Trim();

        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "DIALOG_PHONE_INVALID",
                Message = "Некорректный номер телефона."
            };
        }

        if (string.IsNullOrWhiteSpace(text))
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "DIALOG_TEXT_REQUIRED",
                Message = "Текст ручного сообщения обязателен.",
                Phone = normalizedPhone
            };
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var timezoneOffset = payload.TimezoneOffset ?? await ResolveClientTimezoneOffsetAsync(db, normalizedPhone, cancellationToken);
        if (!timezoneOffset.HasValue)
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "MANUAL_TZ_UNKNOWN",
                Message = "Не удалось определить часовой пояс клиента. Ручная отправка вне очереди заблокирована до актуализации базы клиентов.",
                Phone = normalizedPhone
            };
        }

        if (!TryParseWorkWindowStrict(settings.WorkWindowStart, settings.WorkWindowEnd, out var workStart, out var workEnd))
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "MANUAL_WORK_WINDOW_CONFIG_INVALID",
                Message = "Некорректно задано рабочее окно в настройках. Укажите формат HH:mm и убедитесь, что конец больше начала.",
                Phone = normalizedPhone
            };
        }

        var nowUtc = DateTime.UtcNow;
        if (!IsWithinClientWorkWindow(nowUtc, timezoneOffset.Value, workStart, workEnd, out var localClientTime))
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "MANUAL_OUT_OF_WORK_WINDOW",
                Message = $"Ручная отправка запрещена: у клиента сейчас {localClientTime:HH:mm}, рабочее окно {workStart:HH:mm}-{workEnd:HH:mm}.",
                Phone = normalizedPhone
            };
        }

        var channel = await ResolveChannelForManualSendAsync(db, payload.ChannelId, normalizedPhone, cancellationToken);
        if (channel is null)
        {
            return new DialogManualSendResultDto
            {
                Success = false,
                Code = "CHANNEL_UNAVAILABLE",
                Message = "Нет доступного Android-канала для ручной отправки.",
                Phone = normalizedPhone
            };
        }

        var sendResult = await _traccarSender.SendAsync(new TraccarSmsSendRequest
        {
            Url = channel.Endpoint,
            Token = channel.Token,
            To = normalizedPhone,
            Message = text,
            TimeoutMs = 15000
        }, cancellationToken);

        nowUtc = DateTime.UtcNow;
        var messageRecord = new MessageRecord
        {
            RunJobId = null,
            ClientPhone = normalizedPhone,
            Direction = "out",
            Text = text,
            GatewayStatus = sendResult.Success ? "sent" : "failed",
            CreatedAtUtc = nowUtc,
            MetaJson = JsonSerializer.Serialize(new
            {
                manual = true,
                channelId = channel.Id,
                channelName = channel.Name,
                statusCode = sendResult.StatusCode,
                detail = sendResult.Detail,
                responseBody = sendResult.ResponseBody,
                error = sendResult.Error
            })
        };

        db.Messages.Add(messageRecord);
        if (!sendResult.Success)
        {
            EventService.Append(
                db,
                category: "device",
                eventType: "manual_send_failed",
                severity: "warning",
                message: $"Канал #{channel.Id}: ошибка ручной отправки через gateway. {sendResult.Detail}",
                payload: new
                {
                    phone = normalizedPhone,
                    channelId = channel.Id,
                    channelName = channel.Name,
                    endpoint = channel.Endpoint,
                    statusCode = sendResult.StatusCode,
                    detail = sendResult.Detail,
                    responseBody = sendResult.ResponseBody,
                    error = sendResult.Error
                });
        }

        var hadChannelError = string.Equals(channel.Status, ChannelStatusError, StringComparison.OrdinalIgnoreCase) ||
                              channel.FailStreak > 0 ||
                              channel.Alerted;
        if (sendResult.Success)
        {
            channel.Status = ChannelStatusOnline;
            channel.FailStreak = 0;
            channel.Alerted = false;
            if (hadChannelError)
            {
                await alertService.ResolveChannelAlertsAsync(
                    db,
                    channel.Id,
                    "Канал восстановлен после успешной ручной отправки.",
                    cancellationToken);
            }
        }
        else
        {
            channel.FailStreak = Math.Max(1, channel.FailStreak + 1);
            if (channel.FailStreak >= 3)
            {
                channel.Status = ChannelStatusError;
                channel.Alerted = true;
                await alertService.RaiseChannelErrorAsync(
                    db,
                    channel,
                    "GATEWAY_SEND_FAILED",
                    sendResult.Detail,
                    runSessionId: null,
                    runJobId: null,
                    cancellationToken);
            }
        }

        db.SenderChannels.Update(channel);
        await db.SaveChangesAsync(cancellationToken);

        return new DialogManualSendResultDto
        {
            Success = sendResult.Success,
            Code = sendResult.Success ? "MANUAL_SENT" : "GATEWAY_SEND_FAILED",
            Message = sendResult.Success
                ? "Ручное сообщение отправлено."
                : $"Ошибка ручной отправки: {sendResult.Detail}",
            Phone = normalizedPhone,
            ChannelId = channel.Id,
            ChannelName = channel.Name,
            StatusCode = sendResult.StatusCode,
            MessageId = messageRecord.Id
        };
    }

    public async Task<DialogDraftDto> GetDraftByPhoneAsync(
        AppDbContext db,
        string phone,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogDraftDto
            {
                Phone = string.Empty,
                Text = string.Empty,
                Exists = false,
                UpdatedAtUtc = null
            };
        }

        var row = await db.ManualDialogDrafts
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Phone == normalizedPhone, cancellationToken);

        if (row is null)
        {
            return new DialogDraftDto
            {
                Phone = normalizedPhone,
                Text = string.Empty,
                Exists = false,
                UpdatedAtUtc = null
            };
        }

        return new DialogDraftDto
        {
            Phone = normalizedPhone,
            Text = row.Text ?? string.Empty,
            Exists = true,
            UpdatedAtUtc = row.UpdatedAtUtc
        };
    }

    public async Task<DialogDraftDto> UpsertDraftByPhoneAsync(
        AppDbContext db,
        string phone,
        DialogDraftUpsertRequest payload,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogDraftDto
            {
                Phone = string.Empty,
                Text = string.Empty,
                Exists = false,
                UpdatedAtUtc = null
            };
        }

        var text = (payload.Text ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            await DeleteDraftByPhoneAsync(db, normalizedPhone, cancellationToken);
            return new DialogDraftDto
            {
                Phone = normalizedPhone,
                Text = string.Empty,
                Exists = false,
                UpdatedAtUtc = null
            };
        }

        var nowUtc = DateTime.UtcNow;
        var row = await db.ManualDialogDrafts
            .FirstOrDefaultAsync(x => x.Phone == normalizedPhone, cancellationToken);

        if (row is null)
        {
            row = new ManualDialogDraftRecord
            {
                Phone = normalizedPhone,
                Text = text,
                CreatedAtUtc = nowUtc,
                UpdatedAtUtc = nowUtc
            };
            db.ManualDialogDrafts.Add(row);
        }
        else
        {
            row.Text = text;
            row.UpdatedAtUtc = nowUtc;
            db.ManualDialogDrafts.Update(row);
        }

        await db.SaveChangesAsync(cancellationToken);

        return new DialogDraftDto
        {
            Phone = normalizedPhone,
            Text = text,
            Exists = true,
            UpdatedAtUtc = row.UpdatedAtUtc
        };
    }

    public async Task<DialogDraftDeleteResultDto> DeleteDraftByPhoneAsync(
        AppDbContext db,
        string phone,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogDraftDeleteResultDto
            {
                Phone = string.Empty,
                Deleted = false
            };
        }

        var row = await db.ManualDialogDrafts
            .FirstOrDefaultAsync(x => x.Phone == normalizedPhone, cancellationToken);
        if (row is null)
        {
            return new DialogDraftDeleteResultDto
            {
                Phone = normalizedPhone,
                Deleted = false
            };
        }

        db.ManualDialogDrafts.Remove(row);
        await db.SaveChangesAsync(cancellationToken);
        return new DialogDraftDeleteResultDto
        {
            Phone = normalizedPhone,
            Deleted = true
        };
    }

    public async Task<DialogDeleteResultDto> DeleteByPhoneAsync(
        AppDbContext db,
        string phone,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return new DialogDeleteResultDto { Phone = string.Empty, DeletedMessages = 0 };
        }

        var rows = await db.Messages
            .Where(x => x.ClientPhone == normalizedPhone)
            .ToListAsync(cancellationToken);

        if (rows.Count > 0)
        {
            db.Messages.RemoveRange(rows);
        }

        var draft = await db.ManualDialogDrafts
            .FirstOrDefaultAsync(x => x.Phone == normalizedPhone, cancellationToken);
        if (draft is not null)
        {
            db.ManualDialogDrafts.Remove(draft);
        }

        if (rows.Count > 0 || draft is not null)
        {
            await db.SaveChangesAsync(cancellationToken);
        }

        return new DialogDeleteResultDto
        {
            Phone = normalizedPhone,
            DeletedMessages = rows.Count
        };
    }

    public async Task<DialogPruneResultDto> PruneOlderThanAsync(
        AppDbContext db,
        int olderThanDays,
        CancellationToken cancellationToken)
    {
        var days = olderThanDays <= 0 ? 1 : olderThanDays;
        var thresholdUtc = DateTime.UtcNow.AddDays(-days);

        var rows = await db.Messages
            .Where(x => x.CreatedAtUtc < thresholdUtc)
            .ToListAsync(cancellationToken);

        if (rows.Count > 0)
        {
            db.Messages.RemoveRange(rows);
            await db.SaveChangesAsync(cancellationToken);
        }

        return new DialogPruneResultDto
        {
            OlderThanDays = days,
            ThresholdUtc = thresholdUtc,
            DeletedMessages = rows.Count
        };
    }

    public static ApiErrorDto? ValidateManualSendRequest(DialogManualSendRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Text))
        {
            return new ApiErrorDto { Code = "DIALOG_TEXT_REQUIRED", Message = "Текст ручного сообщения обязателен." };
        }

        if (payload.Text.Trim().Length > 2000)
        {
            return new ApiErrorDto { Code = "DIALOG_TEXT_TOO_LONG", Message = "Текст ручного сообщения не должен превышать 2000 символов." };
        }

        if (payload.ChannelId.HasValue && payload.ChannelId.Value <= 0)
        {
            return new ApiErrorDto { Code = "CHANNEL_ID_INVALID", Message = "channelId должен быть положительным числом." };
        }

        if (payload.TimezoneOffset.HasValue && (payload.TimezoneOffset.Value < -12 || payload.TimezoneOffset.Value > 14))
        {
            return new ApiErrorDto { Code = "TIMEZONE_OFFSET_INVALID", Message = "timezoneOffset должен быть в диапазоне -12..14." };
        }

        return null;
    }

    public static ApiErrorDto? ValidatePruneRequest(DialogPruneRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (payload.OlderThanDays <= 0)
        {
            return new ApiErrorDto { Code = "DIALOG_PRUNE_DAYS_INVALID", Message = "olderThanDays должен быть > 0." };
        }

        if (payload.OlderThanDays > 3650)
        {
            return new ApiErrorDto { Code = "DIALOG_PRUNE_DAYS_TOO_BIG", Message = "olderThanDays не должен превышать 3650." };
        }

        return null;
    }

    public static ApiErrorDto? ValidateDraftUpsertRequest(DialogDraftUpsertRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        var text = (payload.Text ?? string.Empty).Trim();
        if (text.Length > 2000)
        {
            return new ApiErrorDto
            {
                Code = "DIALOG_DRAFT_TEXT_TOO_LONG",
                Message = "Черновик не должен превышать 2000 символов."
            };
        }

        return null;
    }

    private async Task<SenderChannelRecord?> ResolveChannelForManualSendAsync(
        AppDbContext db,
        long? requestedChannelId,
        string normalizedPhone,
        CancellationToken cancellationToken)
    {
        if (requestedChannelId.HasValue && requestedChannelId.Value > 0)
        {
            var explicitChannel = await db.SenderChannels
                .FirstOrDefaultAsync(x => x.Id == requestedChannelId.Value, cancellationToken);

            if (explicitChannel is null)
            {
                return null;
            }

            if (string.Equals(explicitChannel.Status, ChannelStatusError, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(explicitChannel.Status, ChannelStatusOffline, StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }

            return explicitChannel;
        }

        if (!string.IsNullOrWhiteSpace(normalizedPhone))
        {
            var boundChannelId = await db.ClientChannelBindings
                .AsNoTracking()
                .Where(x => x.Phone == normalizedPhone)
                .OrderByDescending(x => x.UpdatedAtUtc)
                .ThenByDescending(x => x.Id)
                .Select(x => (long?)x.ChannelId)
                .FirstOrDefaultAsync(cancellationToken);

            if (boundChannelId.HasValue && boundChannelId.Value > 0)
            {
                var boundChannel = await db.SenderChannels
                    .FirstOrDefaultAsync(x => x.Id == boundChannelId.Value, cancellationToken);
                if (boundChannel is not null &&
                    !string.Equals(boundChannel.Status, ChannelStatusError, StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(boundChannel.Status, ChannelStatusOffline, StringComparison.OrdinalIgnoreCase))
                {
                    return boundChannel;
                }
            }
        }

        var channels = await db.SenderChannels
            .Where(x => x.Status != ChannelStatusError && x.Status != ChannelStatusOffline)
            .OrderBy(x => x.FailStreak)
            .ThenBy(x => x.Id)
            .ToListAsync(cancellationToken);

        if (channels.Count == 0)
        {
            return null;
        }

        var online = channels.FirstOrDefault(x => string.Equals(x.Status, "online", StringComparison.OrdinalIgnoreCase));
        return online ?? channels[0];
    }

    private async Task<int?> ResolveClientTimezoneOffsetAsync(
        AppDbContext db,
        string normalizedPhone,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return null;
        }

        var latestSnapshotId = await db.ClientSnapshots
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .Select(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        if (latestSnapshotId <= 0)
        {
            return null;
        }

        return await db.ClientSnapshotRows
            .AsNoTracking()
            .Where(x => x.SnapshotId == latestSnapshotId && x.Phone == normalizedPhone)
            .OrderByDescending(x => x.CollectedAtUtc)
            .ThenByDescending(x => x.Id)
            .Select(x => (int?)x.TimezoneOffset)
            .FirstOrDefaultAsync(cancellationToken);
    }

    private static bool TryParseWorkWindowStrict(
        string? startRaw,
        string? endRaw,
        out TimeOnly start,
        out TimeOnly end)
    {
        start = default;
        end = default;

        if (TimeOnly.TryParseExact(
                (startRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out start) &&
            TimeOnly.TryParseExact(
                (endRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out end) &&
            end > start)
        {
            return true;
        }

        return false;
    }

    private static bool IsWithinClientWorkWindow(
        DateTime utc,
        int timezoneOffsetFromMoscow,
        TimeOnly start,
        TimeOnly end,
        out TimeOnly localClientTime)
    {
        var clientUtcOffset = TimeSpan.FromHours(3 + timezoneOffsetFromMoscow);
        var local = utc + clientUtcOffset;
        localClientTime = TimeOnly.FromDateTime(local);
        return localClientTime >= start && localClientTime < end;
    }

    private static DialogMessageDto MapMessage(MessageRecord record)
    {
        return new DialogMessageDto
        {
            Id = record.Id,
            Phone = record.ClientPhone,
            Direction = record.Direction,
            Text = record.Text,
            GatewayStatus = record.GatewayStatus ?? string.Empty,
            CreatedAtUtc = record.CreatedAtUtc,
            MetaJson = record.MetaJson ?? string.Empty
        };
    }

    private static string NormalizePhone(string rawPhone)
    {
        var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 10)
        {
            digits = $"7{digits}";
        }
        else if (digits.Length == 11 && digits.StartsWith("8", StringComparison.Ordinal))
        {
            digits = $"7{digits[1..]}";
        }

        if (digits.Length < 10 || digits.Length > 15)
        {
            return string.Empty;
        }

        return $"+{digits}";
    }

    private static string BuildDialogId(string normalizedPhone)
    {
        return normalizedPhone;
    }

    private static void TryReadChannelFromMeta(string? metaJson, out long channelId, out string channelName)
    {
        channelId = 0;
        channelName = string.Empty;

        if (string.IsNullOrWhiteSpace(metaJson))
        {
            return;
        }

        try
        {
            using var doc = JsonDocument.Parse(metaJson);
            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return;
            }

            if (root.TryGetProperty("channelId", out var channelIdNode))
            {
                if (channelIdNode.ValueKind == JsonValueKind.Number && channelIdNode.TryGetInt64(out var parsedChannelId))
                {
                    channelId = parsedChannelId;
                }
                else if (channelIdNode.ValueKind == JsonValueKind.String &&
                         long.TryParse(channelIdNode.GetString(), out parsedChannelId))
                {
                    channelId = parsedChannelId;
                }
            }

            if (root.TryGetProperty("channelName", out var channelNameNode) &&
                channelNameNode.ValueKind == JsonValueKind.String)
            {
                channelName = (channelNameNode.GetString() ?? string.Empty).Trim();
            }
        }
        catch
        {
            // ignore invalid meta json
        }
    }
}
