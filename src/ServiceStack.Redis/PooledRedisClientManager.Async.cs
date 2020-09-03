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
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    public partial class PooledRedisClientManager
        : IRedisClientsManagerAsync
    {
        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetCacheClientAsync(CancellationToken token)
            => new RedisClientManagerCacheClient(this).AsValueTaskResult<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetClientAsync(CancellationToken token)
            => GetClient(true).AsValueTaskResult<IRedisClientAsync>();

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetReadOnlyCacheClientAsync(CancellationToken token)
            => new RedisClientManagerCacheClient(this) { ReadOnly = true }.AsValueTaskResult<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetReadOnlyClientAsync(CancellationToken token)
            => GetReadOnlyClient(true).AsValueTaskResult<IRedisClientAsync>();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }
    }

}