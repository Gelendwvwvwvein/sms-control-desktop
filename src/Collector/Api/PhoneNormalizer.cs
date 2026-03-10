namespace Collector.Api;

public static class PhoneNormalizer
{
    public static string Normalize(
        string? rawPhone,
        int minDigits = 1,
        int maxDigits = 15,
        bool coerceRussianLocalNumbers = false)
    {
        var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
        if (coerceRussianLocalNumbers)
        {
            if (digits.Length == 10)
            {
                digits = $"7{digits}";
            }
            else if (digits.Length == 11 && digits.StartsWith("8", StringComparison.Ordinal))
            {
                digits = $"7{digits[1..]}";
            }
        }

        if (digits.Length < minDigits || digits.Length > maxDigits)
        {
            return string.Empty;
        }

        return digits.Length == 0 ? string.Empty : $"+{digits}";
    }
}
