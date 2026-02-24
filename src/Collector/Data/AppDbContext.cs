using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Data;

public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<SettingRecord> Settings => Set<SettingRecord>();
    public DbSet<ClientSnapshot> ClientSnapshots => Set<ClientSnapshot>();
    public DbSet<ClientSnapshotRow> ClientSnapshotRows => Set<ClientSnapshotRow>();
    public DbSet<TemplateRecord> Templates => Set<TemplateRecord>();
    public DbSet<ManualReplyPresetRecord> ManualReplyPresets => Set<ManualReplyPresetRecord>();
    public DbSet<StopListRecord> StopList => Set<StopListRecord>();
    public DbSet<SenderChannelRecord> SenderChannels => Set<SenderChannelRecord>();
    public DbSet<ClientChannelBindingRecord> ClientChannelBindings => Set<ClientChannelBindingRecord>();
    public DbSet<ClientDebtCacheRecord> ClientDebtCache => Set<ClientDebtCacheRecord>();
    public DbSet<RunSessionRecord> RunSessions => Set<RunSessionRecord>();
    public DbSet<RunJobRecord> RunJobs => Set<RunJobRecord>();
    public DbSet<MessageRecord> Messages => Set<MessageRecord>();
    public DbSet<ManualDialogDraftRecord> ManualDialogDrafts => Set<ManualDialogDraftRecord>();
    public DbSet<AuditLogRecord> AuditLogs => Set<AuditLogRecord>();
    public DbSet<AlertRecord> Alerts => Set<AlertRecord>();
    public DbSet<EventLogRecord> Events => Set<EventLogRecord>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<SettingRecord>(entity =>
        {
            entity.ToTable("settings");
            entity.HasKey(x => x.Key);
            entity.Property(x => x.Key).HasColumnName("key").HasMaxLength(128);
            entity.Property(x => x.ValueJson).HasColumnName("value_json");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
        });

        modelBuilder.Entity<ClientSnapshot>(entity =>
        {
            entity.ToTable("client_snapshots");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.SourceMode).HasColumnName("source_mode").HasMaxLength(32);
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.TotalRows).HasColumnName("total_rows");
            entity.Property(x => x.Notes).HasColumnName("notes");
            entity.HasMany(x => x.Rows).WithOne(x => x.Snapshot).HasForeignKey(x => x.SnapshotId).OnDelete(DeleteBehavior.Cascade);
            entity.HasIndex(x => x.CreatedAtUtc);
        });

        modelBuilder.Entity<ClientSnapshotRow>(entity =>
        {
            entity.ToTable("client_snapshot_rows");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.SnapshotId).HasColumnName("snapshot_id");
            entity.Property(x => x.ExternalClientId).HasColumnName("external_client_id").HasMaxLength(64);
            entity.Property(x => x.Fio).HasColumnName("fio").HasMaxLength(256);
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.TimezoneOffset).HasColumnName("timezone_offset");
            entity.Property(x => x.DaysOverdue).HasColumnName("days_overdue");
            entity.Property(x => x.ContractStatus).HasColumnName("contract_status").HasMaxLength(64);
            entity.Property(x => x.CardUrl).HasColumnName("card_url").HasMaxLength(1024);
            entity.Property(x => x.TotalWithCommissionRaw).HasColumnName("total_with_commission_raw").HasMaxLength(128);
            entity.Property(x => x.CollectedAtUtc).HasColumnName("collected_at_utc");
            entity.HasIndex(x => new { x.SnapshotId, x.Phone });
            entity.HasIndex(x => x.ExternalClientId);
        });

        modelBuilder.Entity<TemplateRecord>(entity =>
        {
            entity.ToTable("templates");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Name).HasColumnName("name").HasMaxLength(256);
            entity.Property(x => x.Kind).HasColumnName("kind").HasMaxLength(64);
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.Text).HasColumnName("text");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.HasIndex(x => x.Kind);
            entity.HasIndex(x => x.Status);
        });

        modelBuilder.Entity<ManualReplyPresetRecord>(entity =>
        {
            entity.ToTable("manual_reply_presets");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Title).HasColumnName("title").HasMaxLength(256);
            entity.Property(x => x.Text).HasColumnName("text");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
        });

        modelBuilder.Entity<StopListRecord>(entity =>
        {
            entity.ToTable("stop_list");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.Reason).HasColumnName("reason").HasMaxLength(512);
            entity.Property(x => x.Source).HasColumnName("source").HasMaxLength(64);
            entity.Property(x => x.AddedAtUtc).HasColumnName("added_at_utc");
            entity.Property(x => x.IsActive).HasColumnName("is_active");
            entity.HasIndex(x => new { x.Phone, x.IsActive });
        });

        modelBuilder.Entity<SenderChannelRecord>(entity =>
        {
            entity.ToTable("sender_channels");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Name).HasColumnName("name").HasMaxLength(128);
            entity.Property(x => x.Endpoint).HasColumnName("endpoint").HasMaxLength(1024);
            entity.Property(x => x.Token).HasColumnName("token").HasMaxLength(1024);
            entity.Property(x => x.SimPhone).HasColumnName("sim_phone").HasMaxLength(32);
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.LastCheckedAtUtc).HasColumnName("last_checked_at_utc");
            entity.Property(x => x.FailStreak).HasColumnName("fail_streak");
            entity.Property(x => x.Alerted).HasColumnName("alerted");
            entity.HasIndex(x => x.Status);
        });

        modelBuilder.Entity<ClientChannelBindingRecord>(entity =>
        {
            entity.ToTable("client_channel_bindings");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.ExternalClientId).HasColumnName("external_client_id").HasMaxLength(64);
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.Property(x => x.LastUsedAtUtc).HasColumnName("last_used_at_utc");
            entity.HasIndex(x => x.ExternalClientId).IsUnique();
            entity.HasIndex(x => x.Phone);
            entity.HasIndex(x => x.ChannelId);
        });

        modelBuilder.Entity<ClientDebtCacheRecord>(entity =>
        {
            entity.ToTable("client_debt_cache");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.ExternalClientId).HasColumnName("external_client_id").HasMaxLength(64);
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.CardUrl).HasColumnName("card_url").HasMaxLength(1024);
            entity.Property(x => x.ExactTotalRaw).HasColumnName("exact_total_raw").HasMaxLength(128);
            entity.Property(x => x.ApproxTotalText).HasColumnName("approx_total_text").HasMaxLength(64);
            entity.Property(x => x.ApproxTotalValue).HasColumnName("approx_total_value");
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.Source).HasColumnName("source").HasMaxLength(32);
            entity.Property(x => x.LastFetchedAtUtc).HasColumnName("last_fetched_at_utc");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.Property(x => x.LastErrorCode).HasColumnName("last_error_code").HasMaxLength(128);
            entity.Property(x => x.LastErrorDetail).HasColumnName("last_error_detail").HasMaxLength(1024);
            entity.HasIndex(x => x.ExternalClientId).IsUnique();
            entity.HasIndex(x => x.Phone);
            entity.HasIndex(x => x.Status);
        });

        modelBuilder.Entity<RunSessionRecord>(entity =>
        {
            entity.ToTable("run_sessions");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Mode).HasColumnName("mode").HasMaxLength(16);
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.StartedAtUtc).HasColumnName("started_at_utc");
            entity.Property(x => x.FinishedAtUtc).HasColumnName("finished_at_utc");
            entity.Property(x => x.SnapshotId).HasColumnName("snapshot_id");
            entity.Property(x => x.FiltersJson).HasColumnName("filters_json");
            entity.Property(x => x.Notes).HasColumnName("notes");
            entity.HasOne(x => x.Snapshot).WithMany().HasForeignKey(x => x.SnapshotId).OnDelete(DeleteBehavior.SetNull);
            entity.HasMany(x => x.Jobs).WithOne(x => x.RunSession).HasForeignKey(x => x.RunSessionId).OnDelete(DeleteBehavior.Cascade);
            entity.HasIndex(x => x.CreatedAtUtc);
            entity.HasIndex(x => x.Status);
        });

        modelBuilder.Entity<RunJobRecord>(entity =>
        {
            entity.ToTable("run_jobs");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.RunSessionId).HasColumnName("run_session_id");
            entity.Property(x => x.ExternalClientId).HasColumnName("external_client_id").HasMaxLength(64);
            entity.Property(x => x.ClientFio).HasColumnName("client_fio").HasMaxLength(256);
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.TzOffset).HasColumnName("tz_offset");
            entity.Property(x => x.DaysOverdue).HasColumnName("days_overdue");
            entity.Property(x => x.TemplateId).HasColumnName("template_id");
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.DeliveryType).HasColumnName("delivery_type").HasMaxLength(32);
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.Attempts).HasColumnName("attempts");
            entity.Property(x => x.MaxAttempts).HasColumnName("max_attempts");
            entity.Property(x => x.PlannedAtUtc).HasColumnName("planned_at_utc");
            entity.Property(x => x.SentAtUtc).HasColumnName("sent_at_utc");
            entity.Property(x => x.LastErrorCode).HasColumnName("last_error_code").HasMaxLength(128);
            entity.Property(x => x.LastErrorDetail).HasColumnName("last_error_detail").HasMaxLength(1024);
            entity.Property(x => x.PayloadJson).HasColumnName("payload_json");
            entity.Property(x => x.PreviewStatus).HasColumnName("preview_status").HasMaxLength(32);
            entity.Property(x => x.PreviewText).HasColumnName("preview_text");
            entity.Property(x => x.PreviewVariablesJson).HasColumnName("preview_variables_json");
            entity.Property(x => x.PreviewUpdatedAtUtc).HasColumnName("preview_updated_at_utc");
            entity.Property(x => x.PreviewErrorCode).HasColumnName("preview_error_code").HasMaxLength(128);
            entity.Property(x => x.PreviewErrorDetail).HasColumnName("preview_error_detail").HasMaxLength(1024);
            entity.HasIndex(x => new { x.RunSessionId, x.Status });
            entity.HasIndex(x => x.Phone);
            entity.HasIndex(x => x.ExternalClientId);
        });

        modelBuilder.Entity<MessageRecord>(entity =>
        {
            entity.ToTable("messages");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.RunJobId).HasColumnName("run_job_id");
            entity.Property(x => x.ClientPhone).HasColumnName("client_phone").HasMaxLength(32);
            entity.Property(x => x.Direction).HasColumnName("direction").HasMaxLength(32);
            entity.Property(x => x.Text).HasColumnName("text");
            entity.Property(x => x.GatewayStatus).HasColumnName("gateway_status").HasMaxLength(64);
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.MetaJson).HasColumnName("meta_json");
            entity.HasIndex(x => x.ClientPhone);
            entity.HasIndex(x => x.CreatedAtUtc);
        });

        modelBuilder.Entity<ManualDialogDraftRecord>(entity =>
        {
            entity.ToTable("manual_dialog_drafts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Phone).HasColumnName("phone").HasMaxLength(32);
            entity.Property(x => x.Text).HasColumnName("text");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.UpdatedAtUtc).HasColumnName("updated_at_utc");
            entity.HasIndex(x => x.Phone).IsUnique();
            entity.HasIndex(x => x.UpdatedAtUtc);
        });

        modelBuilder.Entity<AuditLogRecord>(entity =>
        {
            entity.ToTable("audit_logs");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Category).HasColumnName("category").HasMaxLength(64);
            entity.Property(x => x.Action).HasColumnName("action").HasMaxLength(128);
            entity.Property(x => x.EntityId).HasColumnName("entity_id").HasMaxLength(128);
            entity.Property(x => x.Actor).HasColumnName("actor").HasMaxLength(128);
            entity.Property(x => x.DetailsJson).HasColumnName("details_json");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.HasIndex(x => x.CreatedAtUtc);
            entity.HasIndex(x => x.Category);
            entity.HasIndex(x => x.Actor);
        });

        modelBuilder.Entity<AlertRecord>(entity =>
        {
            entity.ToTable("alerts");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Level).HasColumnName("level").HasMaxLength(32);
            entity.Property(x => x.Text).HasColumnName("text").HasMaxLength(1024);
            entity.Property(x => x.Status).HasColumnName("status").HasMaxLength(32);
            entity.Property(x => x.ChannelId).HasColumnName("channel_id");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.Property(x => x.ClosedAtUtc).HasColumnName("closed_at_utc");
            entity.Property(x => x.MetaJson).HasColumnName("meta_json");
            entity.HasIndex(x => x.Status);
            entity.HasIndex(x => x.ChannelId);
        });

        modelBuilder.Entity<EventLogRecord>(entity =>
        {
            entity.ToTable("events");
            entity.HasKey(x => x.Id);
            entity.Property(x => x.Id).HasColumnName("id");
            entity.Property(x => x.Category).HasColumnName("category").HasMaxLength(64);
            entity.Property(x => x.EventType).HasColumnName("event_type").HasMaxLength(64);
            entity.Property(x => x.Severity).HasColumnName("severity").HasMaxLength(32);
            entity.Property(x => x.Message).HasColumnName("message").HasMaxLength(2048);
            entity.Property(x => x.RunSessionId).HasColumnName("run_session_id");
            entity.Property(x => x.RunJobId).HasColumnName("run_job_id");
            entity.Property(x => x.PayloadJson).HasColumnName("payload_json");
            entity.Property(x => x.CreatedAtUtc).HasColumnName("created_at_utc");
            entity.HasIndex(x => x.CreatedAtUtc);
            entity.HasIndex(x => x.Category);
            entity.HasIndex(x => x.Severity);
        });
    }
}
