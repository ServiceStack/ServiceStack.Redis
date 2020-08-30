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
            => new RedisClientManagerCacheClient(this).AsValueTask<ICacheClientAsync>();

        private ValueTask<ICacheClientAsync> GetReadOnlyCacheClientAsync(in CancellationToken _)
            => ConfigureRedisClientAsync(this.GetReadOnlyClientImpl()).AsValueTask<ICacheClientAsync>();

        private IRedisClientAsync ConfigureRedisClientAsync(IRedisClientAsync client)
            => client;

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetCacheClientAsync(CancellationToken cancellationToken)
            => GetCacheClientAsync(cancellationToken);

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetClientAsync(CancellationToken cancellationToken)
            => GetClientImpl().AsValueTask<IRedisClientAsync>();

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetReadOnlyCacheClientAsync(CancellationToken cancellationToken)
            => GetReadOnlyCacheClientAsync(cancellationToken);

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetReadOnlyClientAsync(CancellationToken cancellationToken)
            => GetReadOnlyClientImpl().AsValueTask<IRedisClientAsync>();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }

        async ValueTask<T> ICacheClientAsync.GetAsync<T>(string key, CancellationToken cancellationToken)
        {
            await using var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.GetAsync<T>(key).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.SetAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask ICacheClientAsync.FlushAllAsync(CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await client.FlushAllAsync(cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<IDictionary<string, T>> ICacheClientAsync.GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            await using var client = await GetReadOnlyCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.GetAllAsync<T>(keys, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask ICacheClientAsync.SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await client.SetAllAsync<T>(values, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.RemoveAsync(string key, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.RemoveAsync(key, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask ICacheClientAsync.RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            await client.RemoveAllAsync(keys, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<long> ICacheClientAsync.IncrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.IncrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<long> ICacheClientAsync.DecrementAsync(string key, uint amount, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.DecrementAsync(key, amount, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, expiresAt, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.AddAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            await using var client = await GetCacheClientAsync(cancellationToken).ConfigureAwait(false);
            return await client.ReplaceAsync<T>(key, value, expiresIn, cancellationToken).ConfigureAwait(false);
        }
    }
}