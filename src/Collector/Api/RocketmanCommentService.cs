using Microsoft.Playwright;

namespace Collector.Api;

public sealed class RocketmanCommentService
{
    private const string LoginPhoneSelector = "#managerloginform-phone";
    private const string LoginPasswordSelector = "#managerloginform-password";
    private const string LoginSubmitPrimary = "/html/body/div[2]/div/div/div/form/div[3]/button";
    private const string LoginSubmitFallback = "form button[type='submit']";
    private const string PostLoginReadySelector = "#w1 > div.clearfix.filter-group-btn > div.pull-right > select";

    private const string CommentTextareaSelector = "#collectorcommentform-message";
    private const string CommentSubmitSelector = "#js-collector-comment-form-submit";
    private const string CommentErrorSelector = "#js-collector-comment-form .help-block-error";
    private const string TotalPrimarySelector = "#w0 > tbody > tr:nth-child(6) > td";
    private const string TotalByLabelSelector = "xpath=//table//tr[.//th[contains(normalize-space(.), 'Итого при оплате с комиссией')]]/td";

    public async Task<RocketmanCommentWriteResult> WriteCommentAsync(
        AppSettingsDto settings,
        RocketmanCommentWriteRequest request,
        CancellationToken cancellationToken)
    {
        var timeoutMs = request.TimeoutMs > 0 ? Math.Min(request.TimeoutMs, 120000) : 30000;

        using var playwright = await Playwright.CreateAsync();
        await using var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = !request.Headed
        });

        var context = await browser.NewContextAsync();
        var page = await context.NewPageAsync();

        try
        {
            await page.GotoAsync(settings.LoginUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = timeoutMs
            });

            await page.Locator(LoginPhoneSelector).First.WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
            await page.Locator(LoginPhoneSelector).First.FillAsync(settings.Login);
            await page.Locator(LoginPasswordSelector).First.FillAsync(settings.Password);

            var loginSubmit = page.Locator($"xpath={LoginSubmitPrimary}").First;
            if (await loginSubmit.CountAsync() > 0)
            {
                await loginSubmit.ClickAsync();
            }
            else
            {
                await page.Locator(LoginSubmitFallback).First.ClickAsync();
            }

            await WaitForPostLoginAsync(page, timeoutMs);

            await page.GotoAsync(request.CardUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = timeoutMs
            });

            var textarea = page.Locator(CommentTextareaSelector).First;
            await textarea.WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
            await textarea.FillAsync(request.Comment);

            var submit = page.Locator(CommentSubmitSelector).First;
            await submit.WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
            await submit.ClickAsync();

            await page.WaitForLoadStateAsync(LoadState.NetworkIdle, new PageWaitForLoadStateOptions
            {
                Timeout = Math.Min(timeoutMs, 15000)
            });

            var errorText = await ReadOptionalTextAsync(page, CommentErrorSelector);
            if (!string.IsNullOrWhiteSpace(errorText))
            {
                return new RocketmanCommentWriteResult
                {
                    Success = false,
                    Code = "COMMENT_VALIDATION_FAILED",
                    Message = errorText,
                    CardUrl = request.CardUrl,
                    CurrentUrl = page.Url,
                    TimeoutMs = timeoutMs
                };
            }

            return new RocketmanCommentWriteResult
            {
                Success = true,
                Code = "COMMENT_SAVED",
                Message = "Комментарий записан в карточку клиента.",
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        catch (TimeoutException ex)
        {
            return new RocketmanCommentWriteResult
            {
                Success = false,
                Code = "COMMENT_TIMEOUT",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        catch (PlaywrightException ex)
        {
            return new RocketmanCommentWriteResult
            {
                Success = false,
                Code = "COMMENT_PLAYWRIGHT_ERROR",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        catch (Exception ex)
        {
            return new RocketmanCommentWriteResult
            {
                Success = false,
                Code = "COMMENT_UNEXPECTED_ERROR",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        finally
        {
            await context.CloseAsync();
            await browser.CloseAsync();
        }
    }

    public async Task<RocketmanCardTotalResult> FetchTotalWithCommissionAsync(
        AppSettingsDto settings,
        RocketmanCardTotalRequest request,
        CancellationToken cancellationToken)
    {
        var timeoutMs = request.TimeoutMs > 0 ? Math.Min(request.TimeoutMs, 120000) : 30000;

        using var playwright = await Playwright.CreateAsync();
        await using var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
        {
            Headless = !request.Headed
        });

        var context = await browser.NewContextAsync();
        var page = await context.NewPageAsync();

        try
        {
            await page.GotoAsync(settings.LoginUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = timeoutMs
            });

            await page.Locator(LoginPhoneSelector).First.WaitForAsync(new LocatorWaitForOptions { Timeout = timeoutMs });
            await page.Locator(LoginPhoneSelector).First.FillAsync(settings.Login);
            await page.Locator(LoginPasswordSelector).First.FillAsync(settings.Password);

            var loginSubmit = page.Locator($"xpath={LoginSubmitPrimary}").First;
            if (await loginSubmit.CountAsync() > 0)
            {
                await loginSubmit.ClickAsync();
            }
            else
            {
                await page.Locator(LoginSubmitFallback).First.ClickAsync();
            }

            await WaitForPostLoginAsync(page, timeoutMs);

            await page.GotoAsync(request.CardUrl, new PageGotoOptions
            {
                WaitUntil = WaitUntilState.DOMContentLoaded,
                Timeout = timeoutMs
            });

            var total = await WaitForNonEmptyTotalAsync(page, timeoutMs);
            if (string.IsNullOrWhiteSpace(total))
            {
                return new RocketmanCardTotalResult
                {
                    Success = false,
                    Code = "DEBT_TOTAL_NOT_FOUND",
                    Message = "Не удалось найти значение «Итого при оплате с комиссией» в карточке клиента.",
                    CardUrl = request.CardUrl,
                    CurrentUrl = page.Url,
                    TimeoutMs = timeoutMs
                };
            }

            return new RocketmanCardTotalResult
            {
                Success = true,
                Code = "DEBT_TOTAL_FOUND",
                Message = "Сумма долга успешно получена из карточки клиента.",
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TotalWithCommissionRaw = total,
                TimeoutMs = timeoutMs
            };
        }
        catch (TimeoutException ex)
        {
            return new RocketmanCardTotalResult
            {
                Success = false,
                Code = "DEBT_FETCH_TIMEOUT",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        catch (PlaywrightException ex)
        {
            return new RocketmanCardTotalResult
            {
                Success = false,
                Code = "DEBT_FETCH_PLAYWRIGHT_ERROR",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        catch (Exception ex)
        {
            return new RocketmanCardTotalResult
            {
                Success = false,
                Code = "DEBT_FETCH_UNEXPECTED_ERROR",
                Message = ex.Message,
                CardUrl = request.CardUrl,
                CurrentUrl = page.Url,
                TimeoutMs = timeoutMs
            };
        }
        finally
        {
            await context.CloseAsync();
            await browser.CloseAsync();
        }
    }

    private static async Task WaitForPostLoginAsync(IPage page, int timeoutMs)
    {
        var start = DateTime.UtcNow;
        while ((DateTime.UtcNow - start).TotalMilliseconds < timeoutMs)
        {
            try
            {
                var url = page.Url ?? string.Empty;
                if (url.Contains("/manager/collector-debt/work", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }

                if (url.Contains("/manager/collector-comment/view", StringComparison.OrdinalIgnoreCase))
                {
                    return;
                }
            }
            catch (PlaywrightException ex) when (IsNavigationContextError(ex))
            {
                await page.WaitForTimeoutAsync(200);
                continue;
            }

            try
            {
                var readyLocator = page.Locator(PostLoginReadySelector).First;
                if (await readyLocator.CountAsync() > 0 && await readyLocator.IsVisibleAsync())
                {
                    return;
                }
            }
            catch (PlaywrightException ex) when (IsNavigationContextError(ex))
            {
                await page.WaitForTimeoutAsync(200);
                continue;
            }

            await page.WaitForTimeoutAsync(200);
        }

        throw new TimeoutException("Не удалось дождаться завершения логина в Rocketman.");
    }

    private static bool IsNavigationContextError(PlaywrightException ex)
    {
        var message = ex.Message ?? string.Empty;
        return message.Contains("Execution context was destroyed", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("Target page, context or browser has been closed", StringComparison.OrdinalIgnoreCase);
    }

    private static async Task<string> ReadOptionalTextAsync(IPage page, string selector)
    {
        try
        {
            var locator = page.Locator(selector).First;
            if (await locator.CountAsync() == 0) return string.Empty;
            var text = await locator.InnerTextAsync();
            return text?.Trim() ?? string.Empty;
        }
        catch
        {
            return string.Empty;
        }
    }

    private static async Task<string> WaitForNonEmptyTotalAsync(IPage page, int timeoutMs)
    {
        var started = DateTime.UtcNow;
        while ((DateTime.UtcNow - started).TotalMilliseconds < timeoutMs)
        {
            var primary = await ReadOptionalTextAsync(page, TotalPrimarySelector);
            var normalizedPrimary = NormalizeText(primary);
            if (LooksLikeDebtValue(normalizedPrimary))
            {
                return normalizedPrimary;
            }

            var byLabel = await ReadOptionalTextAsync(page, TotalByLabelSelector);
            var normalizedByLabel = NormalizeText(byLabel);
            if (LooksLikeDebtValue(normalizedByLabel))
            {
                return normalizedByLabel;
            }

            await page.WaitForTimeoutAsync(250);
        }

        return string.Empty;
    }

    private static string NormalizeText(string? value)
    {
        return (value ?? string.Empty)
            .Replace('\u00A0', ' ')
            .Trim();
    }

    private static bool LooksLikeDebtValue(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Any(char.IsDigit);
    }
}
