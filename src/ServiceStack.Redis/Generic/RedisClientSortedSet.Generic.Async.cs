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
    internal partial class RedisClientSortedSet<T>
        : IRedisSortedSetAsync<T>
    {
        IRedisTypedClientAsync<T> AsyncClient => client;

        IRedisSortedSetAsync<T> AsAsync() => this;

        ValueTask IRedisSortedSetAsync<T>.AddAsync(T item, double score, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSortedSetAsync(this, item, score);

        ValueTask<int> IRedisSortedSetAsync<T>.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetSortedSetCountAsync(this, cancellationToken).AsInt32();

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetAllAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSortedSetAsync(this);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetAllDescendingAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSortedSetDescAsync(this);

        async IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            var count = await AsAsync().CountAsync(cancellationToken).ConfigureAwait(false);
            if (count <= PageLimit)
            {
                var all = await AsyncClient.GetAllItemsFromSortedSetAsync(this, cancellationToken).ConfigureAwait(false);
                foreach (var item in all)
                {
                    yield return item;
                }
            }
            else
            {
                // from GetPagingEnumerator();
                var skip = 0;
                List<T> pageResults;
                do
                {
                    pageResults = await AsyncClient.GetRangeFromSortedSetAsync(this, skip, skip + PageLimit - 1, cancellationToken).ConfigureAwait(false);
                    foreach (var result in pageResults)
                    {
                        yield return result;
                    }
                    skip += PageLimit;
                } while (pageResults.Count == PageLimit);
            }
        }

        ValueTask<double> IRedisSortedSetAsync<T>.GetItemScoreAsync(T item, CancellationToken cancellationToken)
            => AsyncClient.GetItemScoreInSortedSetAsync(this, item, cancellationToken);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetRangeAsync(int fromRank, int toRank, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetAsync(this, fromRank, toRank, cancellationToken);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetRangeByHighestScoreAsync(double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(this, fromScore, toScore, cancellationToken);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetRangeByHighestScoreAsync(double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(this, fromScore, toScore, skip, take, cancellationToken);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetRangeByLowestScoreAsync(double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(this, fromScore, toScore, cancellationToken);

        ValueTask<List<T>> IRedisSortedSetAsync<T>.GetRangeByLowestScoreAsync(double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(this, fromScore, toScore, skip, take, cancellationToken);

        ValueTask<double> IRedisSortedSetAsync<T>.IncrementItemAsync(T item, double incrementBy, CancellationToken cancellationToken)
            => AsyncClient.IncrementItemInSortedSetAsync(this, item, incrementBy, cancellationToken);

        ValueTask<int> IRedisSortedSetAsync<T>.IndexOfAsync(T item, CancellationToken cancellationToken)
            => AsyncClient.GetItemIndexInSortedSetAsync(this, item, cancellationToken).AsInt32();

        ValueTask<long> IRedisSortedSetAsync<T>.IndexOfDescendingAsync(T item, CancellationToken cancellationToken)
            => AsyncClient.GetItemIndexInSortedSetDescAsync(this, item, cancellationToken);

        ValueTask<T> IRedisSortedSetAsync<T>.PopItemWithHighestScoreAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemWithHighestScoreFromSortedSetAsync(this, cancellationToken);

        ValueTask<T> IRedisSortedSetAsync<T>.PopItemWithLowestScoreAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemWithLowestScoreFromSortedSetAsync(this, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithIntersectOfAsync(IRedisSortedSetAsync<T>[] setIds, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSortedSetsAsync(this, setIds, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithIntersectOfAsync(IRedisSortedSetAsync<T>[] setIds, string[] args, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSortedSetsAsync(this, setIds, args, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithUnionOfAsync(IRedisSortedSetAsync<T>[] setIds, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSortedSetsAsync(this, setIds, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithUnionOfAsync(IRedisSortedSetAsync<T>[] setIds, string[] args, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSortedSetsAsync(this, setIds, args, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.RemoveRangeAsync(int minRank, int maxRank, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetAsync(this, minRank, maxRank, cancellationToken);

        ValueTask<long> IRedisSortedSetAsync<T>.RemoveRangeByScoreAsync(double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetByScoreAsync(this, fromScore, toScore, cancellationToken);

        ValueTask IRedisSortedSetAsync<T>.ClearAsync(CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryAsync(setId, cancellationToken).Await();

        ValueTask<bool> IRedisSortedSetAsync<T>.ContainsAsync(T value, CancellationToken cancellationToken)
            => AsyncClient.SortedSetContainsItemAsync(this, value, cancellationToken);

        ValueTask IRedisSortedSetAsync<T>.AddAsync(T value, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSortedSetAsync(this, value, cancellationToken);

        ValueTask<bool> IRedisSortedSetAsync<T>.RemoveAsync(T value, CancellationToken cancellationToken)
            => AsyncClient.RemoveItemFromSortedSetAsync(this, value, cancellationToken).AwaitAsTrue(); // see Remove for why "true"

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithIntersectOfAsync(params IRedisSortedSetAsync<T>[] setIds)
            => AsAsync().PopulateWithIntersectOfAsync(setIds, cancellationToken: default);

        ValueTask<long> IRedisSortedSetAsync<T>.PopulateWithUnionOfAsync(params IRedisSortedSetAsync<T>[] setIds)
            => AsAsync().PopulateWithUnionOfAsync(setIds, cancellationToken: default);
    }
}