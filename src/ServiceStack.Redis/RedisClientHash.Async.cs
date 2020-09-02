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

namespace ServiceStack.Redis
{
    internal partial class RedisClientHash
        : IRedisHashAsync
    {
        private IRedisClientAsync AsyncClient => client;

        ValueTask IRedisHashAsync.AddAsync(KeyValuePair<string, string> item, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashAsync(hashId, item.Key, item.Value, cancellationToken).Await();

        ValueTask IRedisHashAsync.AddAsync(string key, string value, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashAsync(hashId, key, value, cancellationToken).Await();

        ValueTask<bool> IRedisHashAsync.AddIfNotExistsAsync(KeyValuePair<string, string> item, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashIfNotExistsAsync(hashId, item.Key, item.Value, cancellationToken);

        ValueTask IRedisHashAsync.AddRangeAsync(IEnumerable<KeyValuePair<string, string>> items, CancellationToken cancellationToken)
            => AsyncClient.SetRangeInHashAsync(hashId, items, cancellationToken);

        ValueTask IRedisHashAsync.ClearAsync(CancellationToken cancellationToken)
            => new ValueTask(AsyncClient.RemoveAsync(hashId, cancellationToken));

        ValueTask<bool> IRedisHashAsync.ContainsKeyAsync(string key, CancellationToken cancellationToken)
            => AsyncClient.HashContainsEntryAsync(hashId, key, cancellationToken);

        ValueTask<int> IRedisHashAsync.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetHashCountAsync(hashId, cancellationToken).AsInt32();

        IAsyncEnumerator<KeyValuePair<string, string>> IAsyncEnumerable<KeyValuePair<string, string>>.GetAsyncEnumerator(CancellationToken cancellationToken)
            => AsyncClient.ScanAllHashEntriesAsync(hashId).GetAsyncEnumerator(cancellationToken); // note: we're using HSCAN here, not HGETALL

        ValueTask<long> IRedisHashAsync.IncrementValueAsync(string key, int incrementBy, CancellationToken cancellationToken)
            => AsyncClient.IncrementValueInHashAsync(hashId, key, incrementBy, cancellationToken);

        ValueTask<bool> IRedisHashAsync.RemoveAsync(string key, CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryFromHashAsync(hashId, key, cancellationToken);
    }
}