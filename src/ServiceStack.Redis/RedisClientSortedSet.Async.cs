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
    internal partial class RedisClientSortedSet
        : IRedisSortedSetAsync
    {
        private IRedisClientAsync AsyncClient => client;

        ValueTask IRedisSortedSetAsync.AddAsync(string value, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSortedSetAsync(setId, value, cancellationToken).Await();

        private IRedisSortedSetAsync AsAsync() => this;

        ValueTask IRedisSortedSetAsync.ClearAsync(CancellationToken cancellationToken)
            => AsyncClient.RemoveAsync(setId, cancellationToken).Await();

        ValueTask<bool> IRedisSortedSetAsync.ContainsAsync(string value, CancellationToken cancellationToken)
            => AsyncClient.SortedSetContainsItemAsync(setId, value, cancellationToken);

        ValueTask<int> IRedisSortedSetAsync.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetSortedSetCountAsync(setId, cancellationToken).AsInt32();

        ValueTask<List<string>> IRedisSortedSetAsync.GetAllAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSortedSetAsync(setId, cancellationToken);

        async IAsyncEnumerator<string> IAsyncEnumerable<string>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            // uses ZSCAN
            await foreach (var pair in AsyncClient.ScanAllSortedSetItemsAsync(setId, cancellationToken: cancellationToken).ConfigureAwait(false))
            {
                yield return pair.Key;
            }
        }

        ValueTask<long> IRedisSortedSetAsync.GetItemIndexAsync(string value, CancellationToken cancellationToken)
            => AsyncClient.GetItemIndexInSortedSetAsync(setId, value, cancellationToken);

        ValueTask<double> IRedisSortedSetAsync.GetItemScoreAsync(string value, CancellationToken cancellationToken)
            => AsyncClient.GetItemScoreInSortedSetAsync(setId, value, cancellationToken);

        ValueTask<List<string>> IRedisSortedSetAsync.GetRangeAsync(int startingRank, int endingRank, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetAsync(setId, startingRank, endingRank, cancellationToken);

        ValueTask<List<string>> IRedisSortedSetAsync.GetRangeByScoreAsync(string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeByScoreAsync(fromStringScore, toStringScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisSortedSetAsync.GetRangeByScoreAsync(string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(setId, fromStringScore, toStringScore, skip, take, cancellationToken);

        ValueTask<List<string>> IRedisSortedSetAsync.GetRangeByScoreAsync(double fromScore, double toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeByScoreAsync(fromScore, toScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisSortedSetAsync.GetRangeByScoreAsync(double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken);

        ValueTask IRedisSortedSetAsync.IncrementItemScoreAsync(string value, double incrementByScore, CancellationToken cancellationToken)
            => AsyncClient.IncrementItemInSortedSetAsync(setId, value, incrementByScore, cancellationToken).Await();

        ValueTask<string> IRedisSortedSetAsync.PopItemWithHighestScoreAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemWithHighestScoreFromSortedSetAsync(setId, cancellationToken);

        ValueTask<string> IRedisSortedSetAsync.PopItemWithLowestScoreAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemWithLowestScoreFromSortedSetAsync(setId, cancellationToken);

        ValueTask<bool> IRedisSortedSetAsync.RemoveAsync(string value, CancellationToken cancellationToken)
            => AsyncClient.RemoveItemFromSortedSetAsync(setId, value, cancellationToken).AwaitAsTrue(); // see Remove() for why "true"

        ValueTask IRedisSortedSetAsync.RemoveRangeAsync(int fromRank, int toRank, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetAsync(setId, fromRank, toRank, cancellationToken).Await();

        ValueTask IRedisSortedSetAsync.RemoveRangeByScoreAsync(double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetByScoreAsync(setId, fromScore, toScore, cancellationToken).Await();

        ValueTask IRedisSortedSetAsync.StoreFromIntersectAsync(IRedisSortedSetAsync[] ofSets, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSortedSetsAsync(setId, ofSets.GetIds(), cancellationToken).Await();

        ValueTask IRedisSortedSetAsync.StoreFromIntersectAsync(params IRedisSortedSetAsync[] ofSets)
            => AsAsync().StoreFromIntersectAsync(ofSets, cancellationToken: default);

        ValueTask IRedisSortedSetAsync.StoreFromUnionAsync(IRedisSortedSetAsync[] ofSets, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSortedSetsAsync(setId, ofSets.GetIds(), cancellationToken).Await();

        ValueTask IRedisSortedSetAsync.StoreFromUnionAsync(params IRedisSortedSetAsync[] ofSets)
            => AsAsync().StoreFromUnionAsync(ofSets, cancellationToken: default);
    }
}