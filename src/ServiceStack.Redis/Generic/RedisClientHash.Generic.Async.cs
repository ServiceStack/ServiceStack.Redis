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

using ServiceStack.Redis.Internal;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis.Generic
{
    internal partial class RedisClientHash<TKey, T>
        : IRedisHashAsync<TKey, T>
    {
        IRedisTypedClientAsync<T> AsyncClient => client;

        ValueTask IRedisHashAsync<TKey, T>.AddAsync(KeyValuePair<TKey, T> item, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashAsync(this, item.Key, item.Value, cancellationToken).Await();

        ValueTask IRedisHashAsync<TKey, T>.AddAsync(TKey key, T value, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashAsync(this, key, value, cancellationToken).Await();

        ValueTask IRedisHashAsync<TKey, T>.ClearAsync(CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryAsync(new[] { this }, cancellationToken).Await();

        ValueTask<bool> IRedisHashAsync<TKey, T>.ContainsKeyAsync(TKey key, CancellationToken cancellationToken)
            => AsyncClient.HashContainsEntryAsync(this, key, cancellationToken);

        ValueTask<int> IRedisHashAsync<TKey, T>.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetHashCountAsync(this, cancellationToken).AsInt32();

        ValueTask<Dictionary<TKey, T>> IRedisHashAsync<TKey, T>.GetAllAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllEntriesFromHashAsync(this, cancellationToken);

        async IAsyncEnumerator<KeyValuePair<TKey, T>> IAsyncEnumerable<KeyValuePair<TKey, T>>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            var all = await AsyncClient.GetAllEntriesFromHashAsync(this, cancellationToken).ConfigureAwait(false);
            foreach (var pair in all)
            {
                yield return pair;
            }
        }

        ValueTask<bool> IRedisHashAsync<TKey, T>.RemoveAsync(TKey key, CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryFromHashAsync(this, key, cancellationToken);
    }
}