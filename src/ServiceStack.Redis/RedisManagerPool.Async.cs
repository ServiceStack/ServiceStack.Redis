//Copyright (c) Service Stack LLC. All Rights Reserved.
//License: https://raw.github.com/ServiceStack/ServiceStack/master/license.txt

using ServiceStack.Caching;
using ServiceStack.Redis.Internal;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    public partial class RedisManagerPool
        : IRedisClientsManagerAsync
    {
        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetCacheClientAsync(CancellationToken cancellationToken)
            => new RedisClientManagerCacheClient(this).AsValueTask<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetClientAsync(CancellationToken cancellationToken)
            => GetClient(true).AsValueTask<IRedisClientAsync>();

        ValueTask<ICacheClientAsync> IRedisClientsManagerAsync.GetReadOnlyCacheClientAsync(CancellationToken cancellationToken)
            => new RedisClientManagerCacheClient(this) { ReadOnly = true }.AsValueTask<ICacheClientAsync>();

        ValueTask<IRedisClientAsync> IRedisClientsManagerAsync.GetReadOnlyClientAsync(CancellationToken cancellationToken)
            => GetClient(true).AsValueTask<IRedisClientAsync>();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }
    }
}