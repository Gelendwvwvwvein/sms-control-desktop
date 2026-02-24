using Collector.Config;
using Microsoft.Playwright;

namespace Collector.Services;

public static class SelectorHelpers
{
    public static ILocator LocatorFrom(IPage page, string selector)
        => selector.StartsWith("/") ? page.Locator($"xpath={selector}") : page.Locator(selector);

    public static ILocator LocatorFrom(ILocator root, string selector)
        => selector.StartsWith("/") ? root.Locator($"xpath={selector}") : root.Locator(selector);

    public static async Task<string> GetTextAsync(IPage page, SelectorRef sel)
    {
        try
        {
            return (await LocatorFrom(page, sel.Primary).InnerTextAsync()).Trim();
        }
        catch
        {
            if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
            return (await LocatorFrom(page, sel.Fallback!).InnerTextAsync()).Trim();
        }
    }

    public static async Task<string> GetTextAsync(ILocator root, SelectorRef sel)
    {
        try
        {
            return (await LocatorFrom(root, sel.Primary).InnerTextAsync()).Trim();
        }
        catch
        {
            if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
            return (await LocatorFrom(root, sel.Fallback!).InnerTextAsync()).Trim();
        }
    }

    public static async Task<string> GetTextAsync(ILocator root, string selector)
        => (await LocatorFrom(root, selector).InnerTextAsync()).Trim();

    private static async Task<string> TryGetTextFromLocatorAsync(ILocator locator)
    {
        try
        {
            var count = await locator.CountAsync();
            if (count <= 0) return string.Empty;

            var target = count == 1 ? locator : locator.Nth(count - 1);
            var text = await target.InnerTextAsync();
            if (!string.IsNullOrWhiteSpace(text)) return text.Trim();

            var content = await target.TextContentAsync();
            return content?.Trim() ?? string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    public static async Task<string> TryGetTextAsync(IPage page, SelectorRef sel)
    {
        var primary = await TryGetTextFromLocatorAsync(LocatorFrom(page, sel.Primary));
        if (!string.IsNullOrWhiteSpace(primary)) return primary;

        if (!string.IsNullOrWhiteSpace(sel.Fallback))
        {
            var fallback = await TryGetTextFromLocatorAsync(LocatorFrom(page, sel.Fallback!));
            if (!string.IsNullOrWhiteSpace(fallback)) return fallback;
        }

        return string.Empty;
    }

    public static async Task<string> WaitForNonEmptyTextAsync(IPage page, SelectorRef sel, int timeoutMs)
    {
        var start = DateTime.UtcNow;
        while (true)
        {
            var text = await TryGetTextAsync(page, sel);
            if (!string.IsNullOrWhiteSpace(text)) return text;

            if (timeoutMs > 0 && (DateTime.UtcNow - start).TotalMilliseconds >= timeoutMs)
            {
                return text;
            }

            await Task.Delay(250);
        }
    }

    public static async Task<string?> GetAttrAsync(ILocator root, string selector, string attr)
        => await LocatorFrom(root, selector).GetAttributeAsync(attr);

    public static async Task FillAsync(IPage page, SelectorRef sel, string value)
    {
        try
        {
            await LocatorFrom(page, sel.Primary).FillAsync(value);
        }
        catch
        {
            if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
            await LocatorFrom(page, sel.Fallback!).FillAsync(value);
        }
    }

    public static async Task ClickAsync(IPage page, SelectorRef sel)
    {
        try
        {
            await LocatorFrom(page, sel.Primary).ClickAsync();
        }
        catch
        {
            if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
            await LocatorFrom(page, sel.Fallback!).ClickAsync();
        }
    }

    public static async Task SelectOptionAsync(IPage page, SelectorRef sel, string value)
    {
        try
        {
            await LocatorFrom(page, sel.Primary).SelectOptionAsync(new[] { value });
        }
        catch
        {
            if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
            await LocatorFrom(page, sel.Fallback!).SelectOptionAsync(new[] { value });
        }
    }

    public static async Task WaitForAsync(IPage page, SelectorRef sel, int timeoutMs)
    {
        if (timeoutMs > 0)
        {
            try
            {
                await LocatorFrom(page, sel.Primary).WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
                return;
            }
            catch
            {
                if (string.IsNullOrWhiteSpace(sel.Fallback)) throw;
                await LocatorFrom(page, sel.Fallback!).WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
                return;
            }
        }

        while (true)
        {
            if (await IsVisibleAsync(page, sel.Primary)) return;
            if (!string.IsNullOrWhiteSpace(sel.Fallback) && await IsVisibleAsync(page, sel.Fallback!)) return;
            await Task.Delay(250);
        }
    }

    private static async Task<bool> IsVisibleAsync(IPage page, string selector)
    {
        try
        {
            var locator = LocatorFrom(page, selector);
            if (await locator.CountAsync() == 0) return false;
            return await locator.First.IsVisibleAsync();
        }
        catch
        {
            return false;
        }
    }

    public static string ResolveUrl(string baseUrl, string? href)
    {
        if (string.IsNullOrWhiteSpace(href)) return string.Empty;

        var raw = href.Trim();
        if (raw.StartsWith("file://", StringComparison.OrdinalIgnoreCase))
        {
            raw = raw.Replace("file://", "", StringComparison.OrdinalIgnoreCase);
        }

        try { raw = Uri.UnescapeDataString(raw); } catch { /* ignore */ }

        if (raw.StartsWith("//")) raw = "https:" + raw;

        if (raw.StartsWith("http://", StringComparison.OrdinalIgnoreCase) ||
            raw.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
        {
            return raw;
        }

        if (!raw.StartsWith("/")) raw = "/" + raw;
        return new Uri(new Uri(baseUrl), raw).ToString();
    }

    public static decimal? NormalizeAmount(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return null;
        var digits = new string(raw.Where(char.IsDigit).ToArray());
        if (digits.Length == 0) return null;
        if (!decimal.TryParse(digits, out var value)) return null;
        return value;
    }

    public static bool ClassContainsAll(string? classAttr, string requiredTokens)
    {
        if (string.IsNullOrWhiteSpace(classAttr) || string.IsNullOrWhiteSpace(requiredTokens)) return false;
        var tokens = requiredTokens.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        return tokens.All(t => classAttr.Contains(t, StringComparison.Ordinal));
    }
}
