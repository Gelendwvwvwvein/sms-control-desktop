using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class RunDispatchService
{
    private async Task<CommentWriteOutcome> TryWriteContractCommentAfterSendAsync(
        AppDbContext db,
        AppSettingsDto settings,
        RunSessionRecord runSession,
        RunJobRecord job,
        DispatchAttemptResult result,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(runSession.Mode, RunModeLive, StringComparison.OrdinalIgnoreCase))
        {
            return CommentWriteOutcome.NotAttempted("COMMENT_SKIPPED_NON_LIVE", "Запись комментария выполняется только для live-сессий.");
        }

        var cardUrl = ExtractPayloadString(job.PayloadJson, PayloadFieldCardUrl);
        if (string.IsNullOrWhiteSpace(cardUrl))
        {
            var detail = "В payload задачи отсутствует cardUrl для записи комментария.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось записать комментарий (нет cardUrl).",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_CARD_URL_MISSING",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                string.Empty,
                string.Empty,
                "COMMENT_CARD_URL_MISSING",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_CARD_URL_MISSING", detail);
        }

        if (string.IsNullOrWhiteSpace(settings.LoginUrl) ||
            string.IsNullOrWhiteSpace(settings.Login) ||
            string.IsNullOrWhiteSpace(settings.Password))
        {
            var detail = "Не заполнены loginUrl/login/password в настройках.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось записать комментарий (неполные настройки).",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_SETTINGS_MISSING",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                cardUrl,
                string.Empty,
                "COMMENT_SETTINGS_MISSING",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_SETTINGS_MISSING", detail);
        }

        var commentText = ResolveCommentText(result);
        if (string.IsNullOrWhiteSpace(commentText))
        {
            var detail = result.TemplateId.HasValue && result.TemplateId.Value > 0
                ? $"Для шаблона id={result.TemplateId.Value} не заполнен комментарий для записи в договор."
                : "Для задачи не определен шаблон с комментарием для записи в договор.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось определить текст комментария для записи в договор.",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_TEXT_RESOLVE_FAILED",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                cardUrl,
                string.Empty,
                "COMMENT_TEXT_RESOLVE_FAILED",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_TEXT_RESOLVE_FAILED", detail);
        }

        RocketmanCommentWriteResult? last = null;
        for (var attempt = 1; attempt <= CommentWriteMaxAttempts; attempt++)
        {
            last = await rocketmanCommentService.WriteCommentAsync(
                settings,
                new RocketmanCommentWriteRequest
                {
                    CardUrl = cardUrl,
                    Comment = commentText,
                    TimeoutMs = CommentWriteTimeoutMs,
                    Headed = false
                },
                cancellationToken);

            if (last.Success)
            {
                AddEvent(
                    db,
                    runSession.Id,
                    job.Id,
                    "comment_saved",
                    "info",
                    $"Задача #{job.Id}: комментарий записан в карточку клиента.",
                    new
                    {
                        runSessionId = runSession.Id,
                        runJobId = job.Id,
                        cardUrl,
                        commentText,
                        code = last.Code,
                        attempt
                    });
                return CommentWriteOutcome.AttemptSuccess(last.Code, last.Message);
            }

            var retryable = IsRetryableCommentError(last.Code);
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                retryable && attempt < CommentWriteMaxAttempts ? "comment_retry" : "comment_attempt_failed",
                retryable ? "warning" : "error",
                $"Задача #{job.Id}: попытка записи комментария {attempt}/{CommentWriteMaxAttempts} завершилась ошибкой {last.Code}.",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    cardUrl,
                    commentText,
                    code = last.Code,
                    detail = last.Message,
                    retryable,
                    attempt
                });

            if (!retryable || attempt >= CommentWriteMaxAttempts)
            {
                break;
            }

            var pauseMs = attempt * 700;
            await Task.Delay(pauseMs, cancellationToken);
        }

        var failedCode = string.IsNullOrWhiteSpace(last?.Code) ? "COMMENT_WRITE_FAILED" : last!.Code.Trim();
        var failedDetail = string.IsNullOrWhiteSpace(last?.Message)
            ? "Не удалось записать комментарий в карточку клиента."
            : last!.Message.Trim();

        AddEvent(
            db,
            runSession.Id,
            job.Id,
            "comment_failed",
            "error",
            $"Задача #{job.Id}: комментарий не записан после {CommentWriteMaxAttempts} попыток.",
            new
            {
                runSessionId = runSession.Id,
                runJobId = job.Id,
                cardUrl,
                commentText,
                code = failedCode,
                detail = failedDetail
            });

        alertService.RaiseContractCommentError(
            db,
            runSession.Id,
            job.Id,
            job.ExternalClientId,
            job.Phone,
            cardUrl,
            commentText,
            failedCode,
            failedDetail);

        return CommentWriteOutcome.AttemptFailed(failedCode, failedDetail);
    }

    private static string ResolveCommentText(DispatchAttemptResult result)
    {
        return (result.TemplateCommentText ?? string.Empty).Trim();
    }

    private static bool IsRetryableCommentError(string? code)
    {
        var normalized = (code ?? string.Empty).Trim().ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return true;
        }

        return normalized is "COMMENT_TIMEOUT" or "COMMENT_PLAYWRIGHT_ERROR" or "COMMENT_UNEXPECTED_ERROR";
    }

    private static void AddEvent(
        AppDbContext db,
        long runSessionId,
        long? runJobId,
        string eventType,
        string severity,
        string message,
        object payload)
    {
        db.Events.Add(new EventLogRecord
        {
            Category = "dispatch",
            EventType = eventType,
            Severity = severity,
            Message = message,
            RunSessionId = runSessionId,
            RunJobId = runJobId,
            PayloadJson = JsonSerializer.Serialize(payload),
            CreatedAtUtc = DateTime.UtcNow
        });
    }

}
