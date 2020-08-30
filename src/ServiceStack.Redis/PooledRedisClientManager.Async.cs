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
        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetCacheClientAsync(CancellationToken cancellationToken)
            => new RedisClientManagerCacheClient(this).AsValueTask<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetClientAsync(CancellationToken cancellationToken)
            => GetClient(true).AsValueTask<IRedisClientAsync>();

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetReadOnlyCacheClientAsync(CancellationToken cancellationToken)
            => new RedisClientManagerCacheClient(this) { ReadOnly = true }.AsValueTask<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetReadOnlyClientAsync(CancellationToken cancellationToken)
            => GetReadOnlyClient(true).AsValueTask<IRedisClientAsync>();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }
    }

}