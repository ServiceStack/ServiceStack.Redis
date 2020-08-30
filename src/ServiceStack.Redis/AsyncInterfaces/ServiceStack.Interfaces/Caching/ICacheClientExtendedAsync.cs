using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Caching
{
    /// <summary>
    /// Extend ICacheClient API with shared, non-core features
    /// </summary>
    public interface ICacheClientExtendedAsync : ICacheClientAsync
    {
        ValueTask<TimeSpan?> GetTimeToLiveAsync(string key, CancellationToken cancellationToken = default);

        IAsyncEnumerable<string> GetKeysByPatternAsync(string pattern, CancellationToken cancellationToken = default);

        ValueTask RemoveExpiredEntriesAsync(CancellationToken cancellationToken = default);
    }
}