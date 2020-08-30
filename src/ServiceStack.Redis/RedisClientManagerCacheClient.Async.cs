using ServiceStack.Caching;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    partial class RedisClientManagerCacheClient : ICacheClientAsync, IRemoveByPatternAsync, ICacheClientExtendedAsync
    {
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }

        private ValueTask<IRedisClientAsync> GetClientAsync(in CancellationToken cancellationToken)
        {
            AssertNotReadOnly();
            return redisManager.GetClientAsync(cancellationToken);
        }

        async ValueTask<T> ICacheClientAsync.GetAsync<T>(string key, CancellationToken cancellationToken)
        {
            await using var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.GetAsync<T>(key).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask ICacheClientAsync.FlushAllAsync(CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await client.FlushAllAsync(cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<IDictionary<string, T>> ICacheClientAsync.GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            await using var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.GetAllAsync<T>(keys, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask ICacheClientAsync.SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await client.SetAllAsync<T>(values, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.RemoveAsync(string key, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<TimeSpan?> ICacheClientExtendedAsync.GetTimeToLiveAsync(string key, CancellationToken cancellationToken)
        {
            await using var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            if (client is ICacheClientExtendedAsync extended)
            {
                return await extended.GetTimeToLiveAsync(key, cancellationToken).ConfigureAwait(false);
            }
            return null;
            
        }

        async IAsyncEnumerable<string> ICacheClientExtendedAsync.GetKeysByPatternAsync(string pattern, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await using var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            if (client is ICacheClientExtendedAsync extended)
            {
                await foreach (var key in extended.GetKeysByPatternAsync(pattern).WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    yield return key;
                }
            }
        }

        ValueTask ICacheClientExtendedAsync.RemoveExpiredEntriesAsync(CancellationToken cancellationToken)
        {
            //Redis automatically removed expired Cache Entries
            return default;
        }

        async ValueTask IRemoveByPatternAsync.RemoveByPatternAsync(string pattern, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            if (client is IRemoveByPatternAsync redisClient)
            {
                await redisClient.RemoveByPatternAsync(pattern).ConfigureAwait(false);
            }
        }

        async ValueTask IRemoveByPatternAsync.RemoveByRegexAsync(string regex, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            if (client is IRemoveByPatternAsync redisClient)
            {
                await redisClient.RemoveByRegexAsync(regex).ConfigureAwait(false);
            }
        }

        async ValueTask ICacheClientAsync.RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await client.RemoveAllAsync(keys, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<long> ICacheClientAsync.IncrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.IncrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<long> ICacheClientAsync.DecrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.DecrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }
    }
}