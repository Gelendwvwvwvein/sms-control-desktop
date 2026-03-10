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
    private sealed record DispatchChannelChoice(SenderChannelRecord Channel, DateTime NextAvailableAtUtc);
    private sealed class CommentWriteOutcome
    {
        public bool Attempted { get; init; }
        public bool Success { get; init; }
        public string Code { get; init; } = string.Empty;
        public string Detail { get; init; } = string.Empty;

        public static CommentWriteOutcome NotAttempted(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = false,
                Success = false,
                Code = code,
                Detail = detail
            };
        }

        public static CommentWriteOutcome AttemptSuccess(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = true,
                Success = true,
                Code = code,
                Detail = detail
            };
        }

        public static CommentWriteOutcome AttemptFailed(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = true,
                Success = false,
                Code = code,
                Detail = detail
            };
        }
    }

    private sealed class DispatchAttemptResult
    {
        public bool Success { get; init; }
        public bool IsTransient { get; init; }
        public bool CountAsAttempt { get; init; } = true;
        public string Code { get; init; } = string.Empty;
        public string Detail { get; init; } = string.Empty;
        public string MessageText { get; init; } = string.Empty;
        public long? TemplateId { get; init; }
        public string TemplateKind { get; init; } = string.Empty;
        public string TemplateCommentText { get; init; } = string.Empty;
        public bool UsedMessageOverride { get; init; }
        public int StatusCode { get; init; }
        public string ResponseBody { get; init; } = string.Empty;
        public string Error { get; init; } = string.Empty;
        public DateTime? NextPlannedAtUtc { get; init; }
    }

}
