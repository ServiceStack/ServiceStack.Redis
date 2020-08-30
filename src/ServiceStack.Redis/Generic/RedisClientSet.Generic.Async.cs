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
    internal partial class RedisClientSet<T>
        : IRedisSetAsync<T>
    {
        IRedisTypedClientAsync<T> AsyncClient => client;

        ValueTask IRedisSetAsync<T>.AddAsync(T value, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSetAsync(this, value, cancellationToken);

        IRedisSetAsync<T> AsAsync() => this;

        ValueTask IRedisSetAsync<T>.ClearAsync(CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryAsync(setId, cancellationToken).Await();

        ValueTask<bool> IRedisSetAsync<T>.ContainsAsync(T item, CancellationToken cancellationToken)
            => AsyncClient.SetContainsItemAsync(this, item, cancellationToken);

        ValueTask<int> IRedisSetAsync<T>.CountAsync(CancellationToken cancellationToken)
            => AsyncClient.GetSetCountAsync(this, cancellationToken).AsInt32();

        ValueTask<HashSet<T>> IRedisSetAsync<T>.GetAllAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSetAsync(this, cancellationToken);

        async IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            var count = await AsAsync().CountAsync(cancellationToken).ConfigureAwait(false);
            if (count <= PageLimit)
            {
                var all = await AsyncClient.GetAllItemsFromSetAsync(this, cancellationToken).ConfigureAwait(false);
                foreach (var item in all)
                {
                    yield return item;
                }
            }
            else
            {
                // from GetPagingEnumerator
                var skip = 0;
                List<T> pageResults;
                do
                {
                    pageResults = await AsyncClient.GetSortedEntryValuesAsync(this, skip, skip + PageLimit - 1).ConfigureAwait(false);
                    foreach (var result in pageResults)
                    {
                        yield return result;
                    }
                    skip += PageLimit;
                } while (pageResults.Count == PageLimit);
            }
        }

        ValueTask IRedisSetAsync<T>.GetDifferencesAsync(IRedisSetAsync<T>[] withSets, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSetsAsync(this, withSets, cancellationToken);

        ValueTask IRedisSetAsync<T>.GetDifferencesAsync(params IRedisSetAsync<T>[] withSets)
            => AsAsync().GetDifferencesAsync(withSets, cancellationToken: default);

        ValueTask<T> IRedisSetAsync<T>.GetRandomItemAsync(CancellationToken cancellationToken)
            => AsyncClient.GetRandomItemFromSetAsync(this, cancellationToken);

        ValueTask IRedisSetAsync<T>.MoveToAsync(T item, IRedisSetAsync<T> toSet, CancellationToken cancellationToken)
            => AsyncClient.MoveBetweenSetsAsync(this, toSet, item, cancellationToken);

        ValueTask<T> IRedisSetAsync<T>.PopRandomItemAsync(CancellationToken cancellationToken)
            => AsyncClient.PopItemFromSetAsync(this, cancellationToken);

        ValueTask IRedisSetAsync<T>.PopulateWithDifferencesOfAsync(IRedisSetAsync<T> fromSet, IRedisSetAsync<T>[] withSets, CancellationToken cancellationToken)
            => AsyncClient.StoreDifferencesFromSetAsync(this, fromSet, withSets, cancellationToken);

        ValueTask IRedisSetAsync<T>.PopulateWithDifferencesOfAsync(IRedisSetAsync<T> fromSet, params IRedisSetAsync<T>[] withSets)
            => AsAsync().PopulateWithDifferencesOfAsync(fromSet, withSets, cancellationToken: default);

        ValueTask IRedisSetAsync<T>.PopulateWithIntersectOfAsync(IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSetsAsync(this, sets, cancellationToken);

        ValueTask IRedisSetAsync<T>.PopulateWithIntersectOfAsync(params IRedisSetAsync<T>[] sets)
            => AsAsync().PopulateWithIntersectOfAsync(sets, cancellationToken: default);

        ValueTask IRedisSetAsync<T>.PopulateWithUnionOfAsync(IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSetsAsync(this, sets, cancellationToken);

        ValueTask IRedisSetAsync<T>.PopulateWithUnionOfAsync(params IRedisSetAsync<T>[] sets)
            => AsAsync().PopulateWithUnionOfAsync(sets, cancellationToken: default);

        ValueTask<bool> IRedisSetAsync<T>.RemoveAsync(T value, CancellationToken cancellationToken)
            => AsyncClient.RemoveItemFromSetAsync(this, value, cancellationToken).AwaitAsTrue(); // see Remove for why "true"

        ValueTask<List<T>> IRedisSetAsync<T>.SortAsync(int startingFrom, int endingAt, CancellationToken cancellationToken)
            => AsyncClient.GetSortedEntryValuesAsync(this, startingFrom, endingAt, cancellationToken);
    }
}