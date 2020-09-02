using ServiceStack.Caching;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    partial class RedisClientManagerCacheClient : ICacheClientAsync, IRemoveByPatternAsync, IAsyncDisposable
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

        async Task<T> ICacheClientAsync.GetAsync<T>(string key, CancellationToken cancellationToken)
        {
            var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetAsync<T>(key, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.FlushAllAsync(CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.FlushAllAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<IDictionary<string, T>> ICacheClientAsync.GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetAllAsync<T>(keys, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.SetAllAsync<T>(values, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.RemoveAsync(string key, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<TimeSpan?> ICacheClientAsync.GetTimeToLiveAsync(string key, CancellationToken cancellationToken)
        {
            var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetTimeToLiveAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<IEnumerable<string>> ICacheClientAsync.GetKeysByPatternAsync(string pattern, CancellationToken cancellationToken)
        {
            var client = await redisManager.GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetKeysByPatternAsync(pattern, cancellationToken);
            }
        }

        Task ICacheClientAsync.RemoveExpiredEntriesAsync(CancellationToken cancellationToken)
        {
            //Redis automatically removed expired Cache Entries
            return Task.CompletedTask;
        }

        async Task IRemoveByPatternAsync.RemoveByPatternAsync(string pattern, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                if (client is IRemoveByPatternAsync redisClient)
                {
                    await redisClient.RemoveByPatternAsync(pattern, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        async Task IRemoveByPatternAsync.RemoveByRegexAsync(string regex, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                if (client is IRemoveByPatternAsync redisClient)
                {
                    await redisClient.RemoveByRegexAsync(regex, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        async Task ICacheClientAsync.RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.RemoveAllAsync(keys, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<long> ICacheClientAsync.IncrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.IncrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<long> ICacheClientAsync.DecrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.DecrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}