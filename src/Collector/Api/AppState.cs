namespace Collector.Api;

public static class SnapshotModes
{
    public const string Live = "live";
}

public static class RunSessionStatuses
{
    public const string Planned = "planned";
    public const string Running = "running";
    public const string Stopped = "stopped";
    public const string Completed = "completed";
}

public static class RunJobStatuses
{
    public const string Queued = "queued";
    public const string Running = "running";
    public const string Retry = "retry";
    public const string Stopped = "stopped";
    public const string Sent = "sent";
    public const string Failed = "failed";
}

public static class RunJobErrorCodes
{
    public const string StoppedByOperator = "RUN_STOPPED_BY_OPERATOR";
    public const string StoppedByStopList = "STOP_LIST_BLOCKED";
}

public static class PreviewStatuses
{
    public const string Empty = "empty";
    public const string Ready = "ready";
    public const string NeedsDebt = "needs_debt";
    public const string Error = "error";
}

public static class DebtStatuses
{
    public const string Empty = "empty";
    public const string Ready = "ready";
    public const string Error = "error";
}

public static class DebtSources
{
    public const string Snapshot = "snapshot";
    public const string LiveFetch = "live_fetch";
}

public static class DeliveryTypes
{
    public const string Sms = "sms";
}

public static class PayloadFields
{
    public const string MessageOverrideText = "messageOverrideText";
    public const string CardUrl = "cardUrl";
    public const string TotalWithCommissionRaw = "totalWithCommissionRaw";
}

public static class DialogStatuses
{
    public const string None = "none";
}

public static class MessageDirections
{
    public const string Out = "out";
}

public static class MessageGatewayStatuses
{
    public const string Sent = "sent";
    public const string Failed = "failed";
}

public static class ChannelStatuses
{
    public const string Online = "online";
    public const string Offline = "offline";
    public const string Error = "error";
    public const string Unknown = "unknown";
}
