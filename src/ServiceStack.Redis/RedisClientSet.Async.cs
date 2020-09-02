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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    internal partial class RedisClientSet
        : IRedisSetAsync
    {
        private IRedisSetAsync AsAsync() => this;
        private IRedisClientAsync AsyncClient => client;

        ValueTask IRedisSetAsync.AddAsync(string item, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSetAsync(setId, item, cancellationToken);

        ValueTask IRedisSetAsync.ClearAsync(CancellationToken cancellationToken)
            => new ValueTask(AsyncClient.RemoveAsync(setId, cancellationToken));

        ValueTask<bool> IRedisSetAsync.ContainsAsync(string item, CancellationToken cancellationToken)
            => AsyncClient.SetContainsItemAsync(setId, item, cancellationToken);

        ValueTask<int> IRedisSetAsync.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetSetCountAsync(setId, cancellationToken).AsInt32();

        ValueTask<HashSet<string>> IRedisSetAsync.DiffAsync(IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            return AsyncClient.GetDifferencesFromSetAsync(setId, withSetIds, cancellationToken);
        }

        ValueTask<HashSet<string>> IRedisSetAsync.GetAllAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSetAsync(setId, cancellationToken);

        IAsyncEnumerator<string> IAsyncEnumerable<string>.GetAsyncEnumerator(CancellationToken cancellationToken)
            => AsyncClient.ScanAllSetItemsAsync(setId).GetAsyncEnumerator(cancellationToken); // uses SSCAN

        ValueTask<string> IRedisSetAsync.GetRandomEntryAsync(CancellationToken cancellationToken)
            => AsyncClient.GetRandomItemFromSetAsync(setId, cancellationToken);

        ValueTask<List<string>> IRedisSetAsync.GetRangeFromSortedSetAsync(int startingFrom, int endingAt, CancellationToken cancellationToken)
            => AsyncClient.GetSortedEntryValuesAsync(setId, startingFrom, endingAt, cancellationToken);

        ValueTask<HashSet<string>> IRedisSetAsync.IntersectAsync(IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var allSetIds = MergeSetIds(withSets);
            return AsyncClient.GetIntersectFromSetsAsync(allSetIds.ToArray(), cancellationToken);
        }

        ValueTask<HashSet<string>> IRedisSetAsync.IntersectAsync(params IRedisSetAsync[] withSets)
            => AsAsync().IntersectAsync(withSets, cancellationToken: default);

        private List<string> MergeSetIds(IRedisSetAsync[] withSets)
        {
            var allSetIds = new List<string> { setId };
            allSetIds.AddRange(withSets.ToList().ConvertAll(x => x.Id));
            return allSetIds;
        }

        ValueTask IRedisSetAsync.MoveAsync(string value, IRedisSetAsync toSet, CancellationToken cancellationToken)
            => AsyncClient.MoveBetweenSetsAsync(setId, toSet.Id, value, cancellationToken);

        ValueTask<string> IRedisSetAsync.PopAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemFromSetAsync(setId, cancellationToken);

        ValueTask<bool> IRedisSetAsync.RemoveAsync(string item, CancellationToken cancellationToken)
            => AsyncClient.RemoveItemFromSetAsync(setId, item, cancellationToken).AwaitAsTrue(); // see Remove for why true

        ValueTask IRedisSetAsync.StoreDiffAsync(IRedisSetAsync fromSet, IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            return AsyncClient.StoreDifferencesFromSetAsync(setId, fromSet.Id, withSetIds, cancellationToken);
        }

        ValueTask IRedisSetAsync.StoreDiffAsync(IRedisSetAsync fromSet, params IRedisSetAsync[] withSets)
            => AsAsync().StoreDiffAsync(fromSet, withSets, cancellationToken: default);

        ValueTask IRedisSetAsync.StoreIntersectAsync(IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            return AsyncClient.StoreIntersectFromSetsAsync(setId, withSetIds, cancellationToken);
        }

        ValueTask IRedisSetAsync.StoreIntersectAsync(params IRedisSetAsync[] withSets)
            => AsAsync().StoreIntersectAsync(withSets, cancellationToken: default);

        ValueTask IRedisSetAsync.StoreUnionAsync(IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var withSetIds = withSets.ToList().ConvertAll(x => x.Id).ToArray();
            return AsyncClient.StoreUnionFromSetsAsync(setId, withSetIds, cancellationToken);
        }

        ValueTask IRedisSetAsync.StoreUnionAsync(params IRedisSetAsync[] withSets)
            => AsAsync().StoreUnionAsync(withSets, cancellationToken: default);

        ValueTask<HashSet<string>> IRedisSetAsync.UnionAsync(IRedisSetAsync[] withSets, CancellationToken cancellationToken)
        {
            var allSetIds = MergeSetIds(withSets);
            return AsyncClient.GetUnionFromSetsAsync(allSetIds.ToArray(), cancellationToken);
        }

        ValueTask<HashSet<string>> IRedisSetAsync.UnionAsync(params IRedisSetAsync[] withSets)
            => AsAsync().UnionAsync(withSets, cancellationToken: default);
    }
}