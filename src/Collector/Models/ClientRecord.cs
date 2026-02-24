namespace Collector.Models;

public sealed class ClientRecord
{
    public int RowIndex { get; set; }
    public string Fio { get; set; } = "";
    public string Phone { get; set; } = "";
    public string Timezone { get; set; } = "";
    public int DaysOverdue { get; set; }
    public string DaysOverdueRaw { get; set; } = "";
    public string ContractStatusText { get; set; } = "";
    public bool ContractBlueFlag { get; set; }
    public string ContractKey { get; set; } = "";
    public string ClientCardUrl { get; set; } = "";
    public string TotalWithCommissionRaw { get; set; } = "";
    public decimal? TotalWithCommissionValue { get; set; }
    public string Error { get; set; } = "";
}
