using Collector.Config;
using Collector.Models;
using Microsoft.Playwright;
using System.Text.RegularExpressions;
using static Collector.Services.SelectorHelpers;

namespace Collector.Services;

public sealed class RocketmanCollector
{
    private readonly SelectorConfig _cfg;
    private readonly CollectorOptions _opts;
    private readonly string _baseUrl = "https://rocketman.ru";
    private int _debugEmptyCount;
    private int _debugCardCount;
    private int _debugRowLinkCount;

    public RocketmanCollector(SelectorConfig cfg, CollectorOptions opts)
    {
        _cfg = cfg;
        _opts = opts;
    }

    public async Task<List<ClientRecord>> CollectAsync(IPage page, IBrowserContext context, Credentials creds)
    {
        return await CollectCoreAsync(page, context, creds, includeTotals: true);
    }

    public async Task<List<ClientRecord>> CollectTableOnlyAsync(IPage page, Credentials creds)
    {
        return await CollectCoreAsync(page, null, creds, includeTotals: false);
    }

    private async Task<List<ClientRecord>> CollectCoreAsync(IPage page, IBrowserContext? context, Credentials creds, bool includeTotals)
    {
        await DebugLogAsync("start", new { url = page.Url });
        await LoginAsync(page, creds);
        await PrepareClientsTableAsync(page);
        await DebugLogAsync("clients_table_ready", new { url = page.Url });

        var results = await CollectTableRowsAsync(page);

        if (includeTotals)
        {
            if (context is null)
            {
                throw new InvalidOperationException("Browser context is required when includeTotals=true.");
            }

            await FetchTotalsAsync(context, results);
        }

        for (var i = 0; i < results.Count; i++)
        {
            results[i].RowIndex = i + 1;
        }

        return results;
    }

    private async Task FetchTotalsAsync(IBrowserContext context, List<ClientRecord> results)
    {
        var parallel = Math.Max(1, _opts.Parallelism);
        using var sem = new SemaphoreSlim(parallel, parallel);

        var tasks = results.Select(async record =>
        {
            if (string.IsNullOrWhiteSpace(record.ClientCardUrl)) return;

            await sem.WaitAsync();
            try
            {
                record.TotalWithCommissionRaw = await FetchTotalWithCommissionAsync(context, record.ClientCardUrl);
                record.TotalWithCommissionValue = NormalizeAmount(record.TotalWithCommissionRaw);
            }
            catch (Exception ex)
            {
                record.Error = string.IsNullOrWhiteSpace(record.Error) ? ex.Message : record.Error;
            }
            finally
            {
                sem.Release();
            }
        });

        await Task.WhenAll(tasks);
    }

    private async Task LoginAsync(IPage page, Credentials creds)
    {
        if (string.IsNullOrWhiteSpace(_opts.LoginUrl))
            throw new InvalidOperationException("LoginUrl is required. Pass --login-url or set ROCKETMAN_LOGIN_URL.");

        await page.GotoAsync(_opts.LoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle, Timeout = _opts.TimeoutMs });
        await DebugLogAsync("login_page_loaded", new { url = page.Url });

        await FillAsync(page, _cfg.Login.PhoneInput, creds.Phone);
        await FillAsync(page, _cfg.Login.PasswordInput, creds.Password);

        try
        {
            await ClickAsync(page, _cfg.Login.Submit);
        }
        catch
        {
            // Fallback: submit form via Enter
            await LocatorFrom(page, _cfg.Login.PasswordInput.Primary).PressAsync("Enter");
        }

        var ok = await WaitForPostLoginAsync(page);
        await DebugLogAsync("post_login_wait", new { url = page.Url, ok });
        if (!ok)
        {
            var url = page.Url;
            throw new InvalidOperationException($"Не удалось дождаться страницы после логина. Проверьте токен, логин/пароль и доступ. Текущий URL: {url}");
        }
    }

    private async Task<bool> WaitForPostLoginAsync(IPage page)
    {
        var start = DateTime.UtcNow;
        while (true)
        {
            try
            {
                var primary = LocatorFrom(page, _cfg.Login.PostLoginReady.Primary);
                if (await primary.CountAsync() > 0 && await primary.IsVisibleAsync()) return true;
            }
            catch
            {
                // ignore
            }

            try
            {
                if (!string.IsNullOrWhiteSpace(_cfg.Login.PostLoginReady.Fallback))
                {
                    var fallback = LocatorFrom(page, _cfg.Login.PostLoginReady.Fallback!);
                    if (await fallback.CountAsync() > 0 && await fallback.IsVisibleAsync()) return true;
                }
            }
            catch
            {
                // ignore
            }

            try
            {
                var url = page.Url ?? string.Empty;
                if (url.Contains("/collector-debt/work", StringComparison.OrdinalIgnoreCase)) return true;
            }
            catch
            {
                // ignore
            }

            try
            {
                var table = LocatorFrom(page, _cfg.ClientsList.Table.Container);
                if (await table.CountAsync() > 0) return true;
            }
            catch
            {
                // ignore
            }

            if (_opts.TimeoutMs > 0 && (DateTime.UtcNow - start).TotalMilliseconds >= _opts.TimeoutMs)
            {
                return false;
            }

            await page.WaitForTimeoutAsync(250);
        }
    }

    private async Task PrepareClientsTableAsync(IPage page)
    {
        await WaitForAsync(page, _cfg.ClientsList.PageSizeSelect, _opts.TimeoutMs);
        await DebugLogAsync("page_size_select_ready", new { url = page.Url });
        await TrySelectMaxPageSizeAsync(page);
        await ClickAsync(page, _cfg.ClientsList.ApplyButton);

        await LocatorFrom(page, _cfg.ClientsList.Table.Container)
            .WaitForAsync(new LocatorWaitForOptions { Timeout = _opts.TimeoutMs });
        await DebugLogAsync("table_container_ready", new { url = page.Url });
        await WaitForTableDataAsync(page);
    }

    private async Task WaitForTableDataAsync(IPage page)
    {
        var rows = page.Locator(_cfg.ClientsList.Table.Row);
        var start = DateTime.UtcNow;

        while (_opts.TimeoutMs <= 0 || (DateTime.UtcNow - start).TotalMilliseconds < _opts.TimeoutMs)
        {
            try
            {
                if (await rows.CountAsync() > 0)
                {
                    var fioCell = rows.First.Locator(_cfg.ClientsList.Fields.Fio);
                    if (await fioCell.CountAsync() > 0)
                    {
                        var text = (await fioCell.First.InnerTextAsync())?.Trim() ?? string.Empty;
                        if (!string.IsNullOrWhiteSpace(text)) return;
                    }
                }
            }
            catch
            {
                // ignore transient DOM issues
            }

            await page.WaitForTimeoutAsync(250);
        }
    }

    private async Task TrySelectMaxPageSizeAsync(IPage page)
    {
        var selected = await TrySelectMaxPageSizeAsync(page, _cfg.ClientsList.PageSizeSelect.Primary);
        if (!selected && !string.IsNullOrWhiteSpace(_cfg.ClientsList.PageSizeSelect.Fallback))
        {
            selected = await TrySelectMaxPageSizeAsync(page, _cfg.ClientsList.PageSizeSelect.Fallback!);
        }

        if (_opts.Debug)
        {
            await DebugLogAsync("page_size_selected", new
            {
                selected,
                url = page.Url
            });
        }
    }

    private static async Task<bool> TrySelectMaxPageSizeAsync(IPage page, string selector)
    {
        if (string.IsNullOrWhiteSpace(selector))
        {
            return false;
        }

        try
        {
            var select = LocatorFrom(page, selector).First;
            if (await select.CountAsync() == 0)
            {
                return false;
            }

            var maxValue = await select.EvaluateAsync<string?>(
                @"el => {
                    const options = Array.from(el.options || []);
                    const numericValues = options
                        .map(o => (o.value || '').trim())
                        .filter(v => /^\d+$/.test(v))
                        .map(v => Number.parseInt(v, 10))
                        .filter(Number.isFinite);

                    if (!numericValues.length) return null;
                    return String(Math.max(...numericValues));
                }");

            if (string.IsNullOrWhiteSpace(maxValue))
            {
                return false;
            }

            await select.SelectOptionAsync(new[] { maxValue });
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task<List<ClientRecord>> CollectTableRowsAsync(IPage page)
    {
        var map = new Dictionary<string, ClientRecord>(StringComparer.Ordinal);
        var pageIndex = 1;
        var noGrowthPages = 0;

        while (true)
        {
            var before = map.Count;
            await CollectCurrentPageRowsAsync(page, map);
            var expectedTotal = await TryReadExpectedTotalAsync(page);

            if (map.Count == before)
            {
                noGrowthPages++;
                if (noGrowthPages >= 3)
                {
                    await DebugLogAsync("pagination_break_no_growth", new
                    {
                        page = pageIndex,
                        collected = map.Count,
                        url = page.Url
                    });
                    break;
                }
            }
            else
            {
                noGrowthPages = 0;
            }

            if (expectedTotal.HasValue && expectedTotal.Value > 0 && map.Count >= expectedTotal.Value)
            {
                await DebugLogAsync("pagination_break_summary_total", new
                {
                    page = pageIndex,
                    collected = map.Count,
                    expectedTotal = expectedTotal.Value,
                    url = page.Url
                });
                break;
            }

            var hasNext = await TryOpenNextPageAsync(page, pageIndex, map.Count);
            if (!hasNext)
            {
                break;
            }

            pageIndex++;
            await WaitForTableDataAsync(page);
            await page.WaitForTimeoutAsync(300);
        }

        var result = map.Values.ToList();
        if (result.Count > 0 && result.All(x => x.DaysOverdue == 0))
        {
            var nonEmptyRawCount = result.Count(x => !string.IsNullOrWhiteSpace(x.DaysOverdueRaw));
            var sampleRaw = result
                .Select(x => x.DaysOverdueRaw)
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Distinct(StringComparer.Ordinal)
                .Take(10)
                .ToArray();
            await WarnLogAsync("days_overdue_all_zero", new
            {
                total = result.Count,
                nonEmptyRawCount,
                sampleRaw,
                pageUrl = page.Url,
                configuredSelector = _cfg.ClientsList.Fields.DaysOverdue
            });
        }

        return result;
    }

    private async Task CollectCurrentPageRowsAsync(IPage page, Dictionary<string, ClientRecord> map)
    {
        const int maxScrolls = 120;
        var noNewStreak = 0;

        for (var s = 0; s < maxScrolls && noNewStreak < 4; s++)
        {
            await WaitForAsync(page, _cfg.ClientsList.PageSizeSelect, _opts.TimeoutMs);

            var rows = page.Locator(_cfg.ClientsList.Table.Row);
            var container = page.Locator(_cfg.ClientsList.Table.Container);
            var count = await rows.CountAsync();
            var before = map.Count;

            for (var i = 0; i < count; i++)
            {
                var row = rows.Nth(i);
                var record = new ClientRecord();

                try
                {
                    record.Fio = await GetTextAsync(row, _cfg.ClientsList.Fields.Fio);
                    record.Phone = await GetTextAsync(row, _cfg.ClientsList.Fields.Phone);
                    record.Timezone = await GetTextAsync(row, _cfg.ClientsList.Fields.Timezone);
                    record.ContractStatusText = await GetTextSafeAsync(row, _cfg.ClientsList.Fields.ContractStatusText);
                    record.DaysOverdueRaw = await GetTextSafeAsync(row, _cfg.ClientsList.Fields.DaysOverdue);

                    var classAttr = await LocatorFrom(row, _cfg.ClientsList.ContractBlueFlag.Selector)
                        .GetAttributeAsync("class");
                    record.ContractBlueFlag = ClassContainsAll(classAttr, _cfg.ClientsList.ContractBlueFlag.ClassContains);

                    var key = await GetAttrAsync(row, _cfg.ClientsList.Table.RowKey, "href");
                    record.ContractKey = key ?? string.Empty;

                    var href = await GetAttrAsync(row, _cfg.ClientsList.ClientLink.Selector, _cfg.ClientsList.ClientLink.Attr);
                    if (string.IsNullOrWhiteSpace(href))
                    {
                        href = await GetAttrAsync(row, _cfg.ClientsList.ClientLink.Selector, "data-href");
                    }
                    if (string.IsNullOrWhiteSpace(href))
                    {
                        href = await GetAttrAsync(row, _cfg.ClientsList.ClientLink.Selector, "data-url");
                    }
                    record.ClientCardUrl = ResolveUrl(_baseUrl, href);

                    if (TryParseDaysOverdue(record.DaysOverdueRaw, out var parsedDaysOverdue))
                    {
                        record.DaysOverdue = parsedDaysOverdue;
                    }
                    else
                    {
                        record.DaysOverdue = 0;
                        await WarnLogAsync("days_overdue_invalid", new
                        {
                            row = i + 1,
                            raw = record.DaysOverdueRaw,
                            fio = record.Fio,
                            phone = record.Phone,
                            cardUrl = record.ClientCardUrl,
                            pageUrl = page.Url
                        });
                    }

                    if (_opts.Debug && Interlocked.Increment(ref _debugRowLinkCount) <= 3)
                    {
                        await DebugLogAsync("row_link", new
                        {
                            rawHref = href,
                            resolved = record.ClientCardUrl,
                            overdueRaw = record.DaysOverdueRaw,
                            overdue = record.DaysOverdue
                        });
                    }
                }
                catch (PlaywrightException ex) when (ex.Message.Contains("Execution context was destroyed", StringComparison.OrdinalIgnoreCase))
                {
                    // Page navigated or reloaded; break to outer loop and retry.
                    break;
                }
                catch (Exception ex)
                {
                    record.Error = ex.Message;
                }

                var dedupKey = !string.IsNullOrWhiteSpace(record.ClientCardUrl)
                    ? record.ClientCardUrl
                    : (!string.IsNullOrWhiteSpace(record.ContractKey) ? record.ContractKey : $"{record.Fio}|{record.Phone}");

                if (!map.ContainsKey(dedupKey))
                {
                    map[dedupKey] = record;
                }
            }

            noNewStreak = (map.Count == before) ? noNewStreak + 1 : 0;

            await ScrollTableAsync(page, container);
            await page.WaitForTimeoutAsync(350);
        }
    }

    private static async Task<string> GetTextSafeAsync(ILocator row, string selector)
    {
        if (string.IsNullOrWhiteSpace(selector))
        {
            return string.Empty;
        }

        try
        {
            var locator = LocatorFrom(row, selector).First;
            if (await locator.CountAsync() == 0)
            {
                return string.Empty;
            }

            var text = (await locator.InnerTextAsync())?.Trim() ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(text))
            {
                return text;
            }

            return (await locator.TextContentAsync())?.Trim() ?? string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    private static bool TryParseDaysOverdue(string raw, out int value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        var input = raw.Replace('\u00A0', ' ').Trim();

        var slashPattern = Regex.Match(input, @"(?<!\d)(\d{1,3})\s*/\s*\d{1,3}(?!\d)");
        if (slashPattern.Success && int.TryParse(slashPattern.Groups[1].Value, out value))
        {
            return true;
        }

        var overduePattern = Regex.Match(input, @"[Пп]росроч\w*\D{0,20}(\d{1,3})");
        if (overduePattern.Success && int.TryParse(overduePattern.Groups[1].Value, out value))
        {
            return true;
        }

        var plainPattern = Regex.Match(input, @"^\d{1,3}$");
        if (plainPattern.Success && int.TryParse(plainPattern.Value, out value))
        {
            return true;
        }

        var genericNumber = Regex.Match(input, @"(?<!\d)(\d{1,3})(?!\s*[/\.\-]\s*\d)(?!\d)");
        if (genericNumber.Success && int.TryParse(genericNumber.Groups[1].Value, out value))
        {
            return true;
        }

        value = 0;
        return false;
    }

    private async Task<bool> TryOpenNextPageAsync(IPage page, int currentPage, int collectedCount)
    {
        var nextSelector = _cfg.ClientsList.Pagination.NextPage?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(nextSelector))
        {
            return false;
        }

        var next = LocatorFrom(page, nextSelector).First;
        if (await next.CountAsync() == 0)
        {
            return false;
        }

        var disabledClassTokens = _cfg.ClientsList.Pagination.DisabledClassContains?.Trim() ?? string.Empty;
        if (!string.IsNullOrWhiteSpace(disabledClassTokens))
        {
            var classAttr = await next.GetAttributeAsync("class");
            var parentClassAttr = await next.EvaluateAsync<string?>("el => (el.parentElement && el.parentElement.className) ? String(el.parentElement.className) : ''");
            if (ClassContainsAll(classAttr, disabledClassTokens) || ClassContainsAll(parentClassAttr, disabledClassTokens))
            {
                return false;
            }
        }

        var ariaDisabled = (await next.GetAttributeAsync("aria-disabled")) ?? string.Empty;
        if (ariaDisabled.Equals("true", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (!await next.IsEnabledAsync())
        {
            return false;
        }

        try
        {
            await next.ClickAsync(new LocatorClickOptions
            {
                Timeout = _opts.TimeoutMs > 0 ? _opts.TimeoutMs : 15000
            });
        }
        catch
        {
            return false;
        }

        await DebugLogAsync("pagination_next", new
        {
            fromPage = currentPage,
            toPage = currentPage + 1,
            collected = collectedCount,
            url = page.Url
        });

        return true;
    }

    private async Task<int?> TryReadExpectedTotalAsync(IPage page)
    {
        var selector = _cfg.ClientsList.Table.Summary?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(selector))
        {
            return null;
        }

        string text;
        try
        {
            var summary = LocatorFrom(page, selector).First;
            if (await summary.CountAsync() == 0)
            {
                return null;
            }

            text = (await summary.InnerTextAsync())?.Replace('\u00A0', ' ').Trim() ?? string.Empty;
        }
        catch
        {
            return null;
        }

        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var match = Regex.Match(text, @"из\s+(\d+)");
        if (!match.Success || !int.TryParse(match.Groups[1].Value, out var total) || total <= 0)
        {
            return null;
        }

        return total;
    }

    private static async Task ScrollTableAsync(IPage page, ILocator container)
    {
        try
        {
            await container.EvaluateAsync("el => { el.scrollTop = el.scrollHeight; }");
        }
        catch
        {
            // ignore
        }

        try
        {
            await page.EvaluateAsync("() => window.scrollTo(0, document.body.scrollHeight)");
        }
        catch
        {
            // ignore
        }
    }

    private async Task<string> FetchTotalWithCommissionAsync(IBrowserContext context, string url)
    {
        if (string.IsNullOrWhiteSpace(url)) return string.Empty;

        var page = await context.NewPageAsync();
        try
        {
            if (_opts.Debug && Interlocked.Increment(ref _debugCardCount) <= 3)
            {
                await DebugLogAsync("card_fetch_start", new { url });
            }

            await page.GotoAsync(url, new PageGotoOptions { WaitUntil = WaitUntilState.DOMContentLoaded, Timeout = _opts.TimeoutMs });
            if (_opts.Debug && _debugCardCount <= 3)
            {
                await DebugLogAsync("card_page_loaded", new { url, pageUrl = page.Url });
            }
            await WaitForAccrualWidgetAsync(page);
            if (_opts.Debug && _debugCardCount <= 3)
            {
                await DebugLogAsync("card_widget_ready", new { url });
            }
            var value = await ExtractTotalWithCommissionAsync(page);
            if (_opts.Debug && _debugCardCount <= 3)
            {
                await DebugLogAsync("card_fetch_done", new { url, value = value });
            }
            if (string.IsNullOrWhiteSpace(value)) await DebugDumpCommissionAsync(page, url);
            return value;
        }
        catch (Exception ex)
        {
            await DebugDumpCommissionAsync(page, url, ex);
            throw;
        }
        finally
        {
            await page.CloseAsync();
        }
    }

    private async Task<string> ExtractTotalWithCommissionAsync(IPage page)
    {
        var primary = _opts.TimeoutMs <= 0
            ? await TryGetTextAsync(page, _cfg.ClientCard.TotalWithCommission)
            : await WaitForNonEmptyTextAsync(page, _cfg.ClientCard.TotalWithCommission, _opts.TimeoutMs);
        if (!string.IsNullOrWhiteSpace(primary)) return primary;

        var byTh = await TryExtractByThOnceAsync(page, "Итого при оплате с комиссией");
        if (!string.IsNullOrWhiteSpace(byTh)) return byTh;
        var byThUpper = await TryExtractByThOnceAsync(page, "ИТОГО при оплате с комиссией");
        if (!string.IsNullOrWhiteSpace(byThUpper)) return byThUpper;

        var row = await FindCommissionRowAsync(page);
        if (row != null)
        {
            var value = _opts.TimeoutMs <= 0
                ? await ExtractValueFromRowAsync(row)
                : await WaitForRowValueAsync(row, _opts.TimeoutMs);
            if (!string.IsNullOrWhiteSpace(value)) return value;
        }

        // Try to trigger recalculation if widget requires change event
        var triggered = await TriggerAccrualRecalcAsync(page);
        if (_opts.Debug && triggered && _debugCardCount <= 3)
        {
            await DebugLogAsync("card_recalc_triggered", new { url = page.Url });
        }
        if (triggered)
        {
            var retry = await WaitForCommissionValueAsync(page);
            if (!string.IsNullOrWhiteSpace(retry)) return retry;
        }

        return string.Empty;
    }

    private async Task<string> TryExtractByThOnceAsync(IPage page, string label)
    {
        var cell = page.Locator($"#w0 tr:has(th:has-text(\"{label}\")) td").First;
        if (await cell.CountAsync() == 0) return string.Empty;

        var text = (await cell.InnerTextAsync())?.Trim() ?? string.Empty;
        if (text.Any(char.IsDigit)) return text;

        var content = (await cell.TextContentAsync())?.Trim() ?? string.Empty;
        return content.Any(char.IsDigit) ? content : string.Empty;
    }

    private async Task<string> WaitForCommissionValueAsync(IPage page)
    {
        var start = DateTime.UtcNow;
        while (_opts.TimeoutMs <= 0 || (DateTime.UtcNow - start).TotalMilliseconds < _opts.TimeoutMs)
        {
            var primary = await TryGetTextAsync(page, _cfg.ClientCard.TotalWithCommission);
            if (!string.IsNullOrWhiteSpace(primary)) return primary;

            var byTh = await TryExtractByThOnceAsync(page, "Итого при оплате с комиссией");
            if (!string.IsNullOrWhiteSpace(byTh)) return byTh;

            var byThUpper = await TryExtractByThOnceAsync(page, "ИТОГО при оплате с комиссией");
            if (!string.IsNullOrWhiteSpace(byThUpper)) return byThUpper;

            var row = await FindCommissionRowAsync(page);
            if (row != null)
            {
                var value = await ExtractValueFromRowAsync(row);
                if (!string.IsNullOrWhiteSpace(value)) return value;
            }

            await page.WaitForTimeoutAsync(250);
        }

        return string.Empty;
    }

    private static async Task<bool> TriggerAccrualRecalcAsync(IPage page)
    {
        try
        {
            var form = page.Locator("form.js-ajax-loan-calc-search").First;
            if (await form.CountAsync() == 0) return false;

            await form.EvaluateAsync(
                @"form => {
                    form.dispatchEvent(new Event('change', { bubbles: true }));
                    form.dispatchEvent(new Event('input', { bubbles: true }));
                }");
            await page.WaitForTimeoutAsync(800);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task WaitForAccrualWidgetAsync(IPage page)
    {
        var loader = page.Locator(".accrual-widget-loader");
        var table = page.Locator("#w0");
        var start = DateTime.UtcNow;

        while (_opts.TimeoutMs <= 0 || (DateTime.UtcNow - start).TotalMilliseconds < _opts.TimeoutMs)
        {
            try
            {
                if (await table.CountAsync() > 0)
                {
                    var loaderVisible = await loader.CountAsync() > 0 && await loader.IsVisibleAsync();
                    if (!loaderVisible) return;
                }
            }
            catch
            {
                // ignore transient DOM issues
            }

            await page.WaitForTimeoutAsync(250);
        }
    }

    private static async Task<ILocator?> FindCommissionRowAsync(IPage page)
    {
        var exact = page.Locator("tr").Filter(new LocatorFilterOptions { HasTextString = "Итого при оплате с комиссией" }).First;
        if (await exact.CountAsync() > 0) return exact;

        var upper = page.Locator("tr").Filter(new LocatorFilterOptions { HasTextString = "ИТОГО при оплате с комиссией" }).First;
        if (await upper.CountAsync() > 0) return upper;

        var fuzzy = page.Locator("tr:has-text(\"Итого\")").Filter(new LocatorFilterOptions { HasTextString = "комисс" }).First;
        if (await fuzzy.CountAsync() > 0) return fuzzy;

        return null;
    }

    private static async Task<string> WaitForRowValueAsync(ILocator row, int timeoutMs)
    {
        var start = DateTime.UtcNow;
        while (timeoutMs <= 0 || (DateTime.UtcNow - start).TotalMilliseconds < timeoutMs)
        {
            var value = await ExtractValueFromRowAsync(row);
            if (!string.IsNullOrWhiteSpace(value)) return value;
            await Task.Delay(250);
        }

        return string.Empty;
    }

    private static async Task<string> ExtractValueFromRowAsync(ILocator row)
    {
        var cells = row.Locator("td");
        var count = await cells.CountAsync();
        if (count == 0) return string.Empty;

        for (var i = count - 1; i >= 0; i--)
        {
            var cell = cells.Nth(i);
            var text = (await cell.InnerTextAsync())?.Trim() ?? string.Empty;
            if (text.Contains("Итого", StringComparison.OrdinalIgnoreCase)) continue;
            if (text.Any(char.IsDigit)) return text;

            var input = cell.Locator("input").First;
            if (await input.CountAsync() > 0)
            {
                var val = await input.InputValueAsync();
                if (!string.IsNullOrWhiteSpace(val)) return val.Trim();
            }

            var content = (await cell.TextContentAsync())?.Trim() ?? string.Empty;
            if (content.Contains("Итого", StringComparison.OrdinalIgnoreCase)) continue;
            if (content.Any(char.IsDigit)) return content;
        }

        return string.Empty;
    }

    private async Task DebugDumpCommissionAsync(IPage page, string url, Exception? ex = null)
    {
        if (!_opts.Debug) return;
        if (Interlocked.Increment(ref _debugEmptyCount) > 5) return;

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_opts.DebugLogPath) ?? ".");

            var hasTable = await page.Locator("#w0").CountAsync() > 0;
            var pageUrl = page.Url;
            var title = string.Empty;
            try { title = await page.TitleAsync(); } catch { /* ignore */ }

            var loginVisible = false;
            try
            {
                loginVisible = await page.Locator(_cfg.Login.PhoneInput.Primary).IsVisibleAsync();
            }
            catch
            {
                // ignore
            }
            var tableHtml = string.Empty;
            if (hasTable)
            {
                try { tableHtml = await page.Locator("#w0").InnerHTMLAsync(); } catch { /* ignore */ }
            }

            var row = await FindCommissionRowAsync(page);
            var rowHtml = string.Empty;
            if (row != null)
            {
                try { rowHtml = await row.InnerHTMLAsync(); } catch { /* ignore */ }
            }

            var loaderVisible = false;
            try
            {
                var loader = page.Locator(".accrual-widget-loader");
                loaderVisible = await loader.IsVisibleAsync();
            }
            catch
            {
                // ignore
            }

            string cellByTh = string.Empty;
            try { cellByTh = await TryExtractByThOnceAsync(page, "Итого при оплате с комиссией"); } catch { /* ignore */ }

            var snippet = new
            {
                ts = DateTime.UtcNow.ToString("O"),
                url,
                pageUrl,
                title,
                loginVisible,
                hasTable,
                loaderVisible,
                rowHtml,
                tableHtml,
                cellByTh,
                error = ex?.ToString()
            };

            var json = System.Text.Json.JsonSerializer.Serialize(snippet);
            await File.AppendAllTextAsync(_opts.DebugLogPath, json + Environment.NewLine);
        }
        catch
        {
            // ignore debug failures
        }
    }

    private async Task DebugLogAsync(string step, object data)
    {
        if (!_opts.Debug) return;

        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_opts.DebugLogPath) ?? ".");

            var payload = new
            {
                ts = DateTime.UtcNow.ToString("O"),
                step,
                data
            };

            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            await File.AppendAllTextAsync(_opts.DebugLogPath, json + Environment.NewLine);
        }
        catch
        {
            // ignore
        }
    }

    private async Task WarnLogAsync(string code, object data)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(_opts.DebugLogPath) ?? ".");

            var payload = new
            {
                ts = DateTime.UtcNow.ToString("O"),
                level = "WARN",
                code,
                data
            };

            var json = System.Text.Json.JsonSerializer.Serialize(payload);
            Console.Error.WriteLine(json);
            await File.AppendAllTextAsync(_opts.DebugLogPath, json + Environment.NewLine);
        }
        catch
        {
            // ignore
        }
    }
}
