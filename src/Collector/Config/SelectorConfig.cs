namespace Collector.Config;

public sealed class SelectorConfig
{
    public LoginSelectors Login { get; set; } = new();
    public ClientsListSelectors ClientsList { get; set; } = new();
    public ClientCardSelectors ClientCard { get; set; } = new();
}

public sealed class SelectorRef
{
    public string Primary { get; set; } = "";
    public string? Fallback { get; set; }
}

public sealed class LoginSelectors
{
    public SelectorRef PhoneInput { get; set; } = new();
    public SelectorRef PasswordInput { get; set; } = new();
    public SelectorRef Submit { get; set; } = new();
    public SelectorRef PostLoginReady { get; set; } = new();
}

public sealed class ClientsListSelectors
{
    public SelectorRef PageSizeSelect { get; set; } = new();
    public SelectorRef ApplyButton { get; set; } = new();
    public TableSelectors Table { get; set; } = new();
    public ClientFieldSelectors Fields { get; set; } = new();
    public ContractBlueFlagSelector ContractBlueFlag { get; set; } = new();
    public ClientLinkSelector ClientLink { get; set; } = new();
    public PaginationSelectors Pagination { get; set; } = new();
}

public sealed class TableSelectors
{
    public string Container { get; set; } = "";
    public string Row { get; set; } = "";
    public string RowKey { get; set; } = "";
    public string Summary { get; set; } = "";
}

public sealed class ClientFieldSelectors
{
    public string Fio { get; set; } = "";
    public string Phone { get; set; } = "";
    public string Timezone { get; set; } = "";
    public string DaysOverdue { get; set; } = "";
    public string ContractStatusText { get; set; } = "";
}

public sealed class ContractBlueFlagSelector
{
    public string Selector { get; set; } = "";
    public string ClassContains { get; set; } = "";
}

public sealed class ClientLinkSelector
{
    public string Selector { get; set; } = "";
    public string Attr { get; set; } = "href";
}

public sealed class PaginationSelectors
{
    public string NextPage { get; set; } = "";
    public string DisabledClassContains { get; set; } = "disabled";
}

public sealed class ClientCardSelectors
{
    public SelectorRef TotalWithCommission { get; set; } = new();
}
