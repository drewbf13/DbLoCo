using Microsoft.Data.SqlClient;
using System.ComponentModel;

namespace SqlClone.Infrastructure.SqlServer;

internal static class SqlClientTransientRetry
{
    private const int DecryptNativeErrorCode = unchecked((int)0x80090330);

    public static bool IsTransientTransportError(Exception exception)
    {
        if (exception is SqlException sqlException)
        {
            var message = sqlException.Message;
            if (message.Contains("transport-level error", StringComparison.OrdinalIgnoreCase)
                || message.Contains("SSL Provider", StringComparison.OrdinalIgnoreCase)
                || message.Contains("could not be decrypted", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            if (sqlException.InnerException is Win32Exception win32Exception
                && win32Exception.NativeErrorCode == DecryptNativeErrorCode)
            {
                return true;
            }
        }

        return exception.InnerException is not null && IsTransientTransportError(exception.InnerException);
    }

    public static async Task OpenWithRetryAsync(SqlConnection connection, int maxAttempts, CancellationToken cancellationToken)
    {
        if (maxAttempts < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(maxAttempts), "maxAttempts must be greater than zero.");
        }

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await connection.OpenAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts && IsTransientTransportError(ex))
            {
                SqlConnection.ClearPool(connection);
                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, attempt - 1)), cancellationToken);
            }
        }
    }
}
