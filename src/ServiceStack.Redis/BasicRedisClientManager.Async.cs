//
// https://github.com/ServiceStack/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2013 Service Stack LLC. All Rights Reserved.
//
// Licensed under the same terms of ServiceStack.
//

using ServiceStack.Caching;
using ServiceStack.Redis.Internal;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Provides thread-safe retrieval of redis clients since each client is a new one.
    /// Allows the configuration of different ReadWrite and ReadOnly hosts
    /// </summary>
    public partial class BasicRedisClientManager
        : IRedisClientsManagerAsync, ICacheClientAsync
    {
        private ValueTask<ICacheClientAsync> GetCacheClientAsync(in CancellationToken _)
            => new RedisClientManagerCacheClient(this).AsValueTaskResult<ICacheClientAsync>();

        private ValueTask<ICacheClientAsync> GetReadOnlyCacheClientAsync(in CancellationToken _)
            => ConfigureRedisClientAsync(this.GetReadOnlyClientImpl()).AsValueTaskResult<ICacheClientAsync>();

        private IRedisClientAsync ConfigureRedisClientAsync(IRedisClientAsync client)
            => client;

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetCacheClientAsync(CancellationToken cancellationToken)
            => GetCacheClientAsync(cancellationToken);

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetClientAsync(CancellationToken cancellationToken)
            => GetClientImpl().AsValueTaskResult<IRedisClientAsync>();

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetReadOnlyCacheClientAsync(CancellationToken cancellationToken)
            => GetReadOnlyCacheClientAsync(cancellationToken);

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetReadOnlyClientAsync(CancellationToken cancellationToken)
            => GetReadOnlyClientImpl().AsValueTaskResult<IRedisClientAsync>();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }

        async Task<T> ICacheClientAsync.GetAsync<T>(string key, CancellationToken cancellationToken)
        {
            var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetAsync<T>(key).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.SetAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.FlushAllAsync(CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.FlushAllAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<IDictionary<string, T>> ICacheClientAsync.GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetAllAsync<T>(keys, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.SetAllAsync<T>(values, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.RemoveAsync(string key, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.RemoveAllAsync(keys, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<long> ICacheClientAsync.IncrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.IncrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<long> ICacheClientAsync.DecrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.DecrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.AddAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.ReplaceAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<TimeSpan?> ICacheClientAsync.GetTimeToLiveAsync(string key, CancellationToken cancellationToken)
        {
            var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetTimeToLiveAsync(key, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task<IEnumerable<string>> ICacheClientAsync.GetKeysByPatternAsync(string pattern, CancellationToken cancellationToken)
        {
            var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                return await client.GetKeysByPatternAsync(pattern, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task ICacheClientAsync.RemoveExpiredEntriesAsync(CancellationToken cancellationToken)
        {
            var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await using (client as IAsyncDisposable)
            {
                await client.RemoveExpiredEntriesAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}