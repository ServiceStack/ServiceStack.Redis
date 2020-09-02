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

using ServiceStack.Data;
using ServiceStack.Model;
using ServiceStack.Redis.Internal;
using ServiceStack.Text;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis.Generic
{
    partial class RedisTypedClient<T>
        : IRedisTypedClientAsync<T>
    {
        public IRedisTypedClientAsync<T> AsAsync() => this;

        private IRedisClientAsync AsyncClient => client;
        private IRedisNativeClientAsync AsyncNative => client;

        IRedisSetAsync IRedisTypedClientAsync<T>.TypeIdsSet => TypeIdsSetRaw;

        IRedisClientAsync IRedisTypedClientAsync<T>.RedisClient => client;

        internal ValueTask ExpectQueuedAsync(CancellationToken cancellationToken)
            => client.ExpectQueuedAsync(cancellationToken);

        internal ValueTask ExpectOkAsync(CancellationToken cancellationToken)
            => client.ExpectOkAsync(cancellationToken);

        internal ValueTask<int> ReadMultiDataResultCountAsync(CancellationToken cancellationToken)
            => client.ReadMultiDataResultCountAsync(cancellationToken);

        ValueTask<T> IRedisTypedClientAsync<T>.GetValueAsync(string key, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.GetAsync(key, cancellationToken));

        async ValueTask IRedisTypedClientAsync<T>.SetValueAsync(string key, T entity, CancellationToken cancellationToken)
        {
            AssertNotNull(key);
            await AsyncClient.SetAsync(key, SerializeValue(entity), cancellationToken).ConfigureAwait(false);
            await client.RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);
        }

        Task<T> IEntityStoreAsync<T>.GetByIdAsync(object id, CancellationToken cancellationToken)
        {
            var key = client.UrnKey<T>(id);
            return AsAsync().GetValueAsync(key, cancellationToken).AsTask();
        }

        internal ValueTask FlushSendBufferAsync(CancellationToken cancellationToken)
            => client.FlushSendBufferAsync(cancellationToken);

        internal ValueTask AddTypeIdsRegisteredDuringPipelineAsync(CancellationToken cancellationToken)
            => client.AddTypeIdsRegisteredDuringPipelineAsync(cancellationToken);

        async Task<IList<T>> IEntityStoreAsync<T>.GetByIdsAsync(IEnumerable ids, CancellationToken cancellationToken)
        {
            if (ids != null)
            {
                var urnKeys = ids.Map(x => client.UrnKey<T>(x));
                if (urnKeys.Count != 0)
                    return await AsAsync().GetValuesAsync(urnKeys, cancellationToken).ConfigureAwait(false);
            }

            return new List<T>();
        }

        async Task<IList<T>> IEntityStoreAsync<T>.GetAllAsync(CancellationToken cancellationToken)
        {
            var allKeys = await AsyncClient.GetAllItemsFromSetAsync(this.TypeIdsSetKey, cancellationToken).ConfigureAwait(false);
            return await AsAsync().GetByIdsAsync(allKeys.ToArray(), cancellationToken).ConfigureAwait(false);
        }

        async Task<T> IEntityStoreAsync<T>.StoreAsync(T entity, CancellationToken cancellationToken)
        {
            var urnKey = client.UrnKey(entity);
            await AsAsync().SetValueAsync(urnKey, entity, cancellationToken).ConfigureAwait(false);
            return entity;
        }

        async Task IEntityStoreAsync<T>.StoreAllAsync(IEnumerable<T> entities, CancellationToken cancellationToken)
        {
            if (PrepareStoreAll(entities, out var keys, out var values, out var entitiesList))
            {
                await AsyncNative.MSetAsync(keys, values, cancellationToken).ConfigureAwait(false);
                await client.RegisterTypeIdsAsync(entitiesList, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task IEntityStoreAsync<T>.DeleteAsync(T entity, CancellationToken cancellationToken)
        {
            var urnKey = client.UrnKey(entity);
            await AsyncClient.RemoveEntryAsync(new[] { urnKey }, cancellationToken).ConfigureAwait(false);
            await client.RemoveTypeIdsAsync(new[] { entity }, cancellationToken).ConfigureAwait(false);
        }

        async Task IEntityStoreAsync<T>.DeleteByIdAsync(object id, CancellationToken cancellationToken)
        {
            var urnKey = client.UrnKey<T>(id);

            await AsyncClient.RemoveEntryAsync(new[] { urnKey }, cancellationToken).ConfigureAwait(false);
            await client.RemoveTypeIdsAsync<T>(new[] { id.ToString() }, cancellationToken).ConfigureAwait(false);
        }

        async Task IEntityStoreAsync<T>.DeleteByIdsAsync(IEnumerable ids, CancellationToken cancellationToken)
        {
            if (ids == null) return;

            var urnKeys = ids.Map(t => client.UrnKey<T>(t));
            if (urnKeys.Count > 0)
            {
                await AsyncClient.RemoveEntryAsync(urnKeys.ToArray(), cancellationToken).ConfigureAwait(false);
                await client.RemoveTypeIdsAsync<T>(ids.Map(x => x.ToString()).ToArray(), cancellationToken).ConfigureAwait(false);
            }
        }

        async Task IEntityStoreAsync<T>.DeleteAllAsync(CancellationToken cancellationToken)
        {
            var ids = await AsyncClient.GetAllItemsFromSetAsync(this.TypeIdsSetKey, cancellationToken).ConfigureAwait(false);
            var urnKeys = ids.Map(t => client.UrnKey<T>(t));
            if (urnKeys.Count > 0)
            {
                await AsyncClient.RemoveEntryAsync(urnKeys.ToArray(), cancellationToken).ConfigureAwait(false);
                await AsyncClient.RemoveEntryAsync(new[] { this.TypeIdsSetKey }, cancellationToken).ConfigureAwait(false);
            }
        }

        async ValueTask<List<T>> IRedisTypedClientAsync<T>.GetValuesAsync(List<string> keys, CancellationToken cancellationToken)
        {
            if (keys.IsNullOrEmpty()) return new List<T>();

            var resultBytesArray = await AsyncNative.MGetAsync(keys.ToArray(), cancellationToken).ConfigureAwait(false);
            return ProcessGetValues(resultBytesArray);
        }

        ValueTask<IRedisTypedTransactionAsync<T>> IRedisTypedClientAsync<T>.CreateTransactionAsync(CancellationToken cancellationToken)
        {
            IRedisTypedTransactionAsync<T> obj = new RedisTypedTransaction<T>(this, true);
            return obj.AsValueTaskResult();
        }

        IRedisTypedPipelineAsync<T> IRedisTypedClientAsync<T>.CreatePipeline()
            => new RedisTypedPipeline<T>(this);


        ValueTask<IAsyncDisposable> IRedisTypedClientAsync<T>.AcquireLockAsync(TimeSpan? timeOut, CancellationToken cancellationToken)
            => AsyncClient.AcquireLockAsync(this.TypeLockKey, timeOut, cancellationToken);

        long IRedisTypedClientAsync<T>.Db => AsyncClient.Db;

        IHasNamed<IRedisListAsync<T>> IRedisTypedClientAsync<T>.Lists => Lists as IHasNamed<IRedisListAsync<T>> ?? throw new NotSupportedException("The provided Lists does not support IRedisListAsync");
        IHasNamed<IRedisSetAsync<T>> IRedisTypedClientAsync<T>.Sets => Sets as IHasNamed<IRedisSetAsync<T>> ?? throw new NotSupportedException("The provided Sets does not support IRedisSetAsync");
        IHasNamed<IRedisSortedSetAsync<T>> IRedisTypedClientAsync<T>.SortedSets => SortedSets as IHasNamed<IRedisSortedSetAsync<T>> ?? throw new NotSupportedException("The provided SortedSets does not support IRedisSortedSetAsync");

        IRedisHashAsync<TKey, T> IRedisTypedClientAsync<T>.GetHash<TKey>(string hashId) => GetHash<TKey>(hashId) as IRedisHashAsync<TKey, T> ?? throw new NotSupportedException("The provided Hash does not support IRedisHashAsync");

        ValueTask IRedisTypedClientAsync<T>.SelectAsync(long db, CancellationToken cancellationToken)
            => AsyncClient.SelectAsync(db, cancellationToken);

        ValueTask<List<string>> IRedisTypedClientAsync<T>.GetAllKeysAsync(CancellationToken cancellationToken)
            => AsyncClient.GetAllKeysAsync(cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.SetSequenceAsync(int value, CancellationToken cancellationToken)
            => AsyncNative.GetSetAsync(SequenceKey, Encoding.UTF8.GetBytes(value.ToString()), cancellationToken).Await();

        ValueTask<long> IRedisTypedClientAsync<T>.GetNextSequenceAsync(CancellationToken cancellationToken)
            => AsAsync().IncrementValueAsync(SequenceKey, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetNextSequenceAsync(int incrBy, CancellationToken cancellationToken)
            => AsAsync().IncrementValueByAsync(SequenceKey, incrBy, cancellationToken);

        ValueTask<RedisKeyType> IRedisTypedClientAsync<T>.GetEntryTypeAsync(string key, CancellationToken cancellationToken)
            => AsyncClient.GetEntryTypeAsync(key, cancellationToken);

        ValueTask<string> IRedisTypedClientAsync<T>.GetRandomKeyAsync(CancellationToken cancellationToken)
            => AsyncClient.GetRandomKeyAsync(cancellationToken);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void AssertNotNull(object obj, string name = "key")
        {
            if (obj is null) Throw(name);
            static void Throw(string name) => throw new ArgumentNullException(name);
        }

        async ValueTask IRedisTypedClientAsync<T>.SetValueAsync(string key, T entity, TimeSpan expireIn, CancellationToken cancellationToken)
        {
            AssertNotNull(key);
            await AsyncClient.SetAsync(key, SerializeValue(entity)).ConfigureAwait(false);
            await client.RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> IRedisTypedClientAsync<T>.SetValueIfNotExistsAsync(string key, T entity, CancellationToken cancellationToken)
        {
            var success = await AsyncNative.SetNXAsync(key, SerializeValue(entity)).IsSuccessAsync().ConfigureAwait(false);
            if (success) await client.RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);
            return success;
        }

        async ValueTask<bool> IRedisTypedClientAsync<T>.SetValueIfExistsAsync(string key, T entity, CancellationToken cancellationToken)
        {
            var success = await AsyncNative.SetAsync(key, SerializeValue(entity), exists: true).ConfigureAwait(false);
            if (success) await client.RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);
            return success;
        }

        async ValueTask<T> IRedisTypedClientAsync<T>.StoreAsync(T entity, TimeSpan expireIn, CancellationToken cancellationToken)
        {
            var urnKey = client.UrnKey(entity);
            await AsAsync().SetValueAsync(urnKey, entity, cancellationToken).ConfigureAwait(false);
            return entity;
        }

        ValueTask<T> IRedisTypedClientAsync<T>.GetAndSetValueAsync(string key, T value, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.GetSetAsync(key, SerializeValue(value), cancellationToken));

        ValueTask<bool> IRedisTypedClientAsync<T>.ContainsKeyAsync(string key, CancellationToken cancellationToken)
            => AsyncNative.ExistsAsync(key, cancellationToken).IsSuccessAsync();

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryAsync(string key, CancellationToken cancellationToken)
            => AsyncNative.DelAsync(key, cancellationToken).IsSuccessAsync();

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryAsync(string[] keys, CancellationToken cancellationToken)
            => AsyncNative.DelAsync(keys, cancellationToken).IsSuccessAsync();

        async ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryAsync(IHasStringId[] entities, CancellationToken cancellationToken)
        {
            var ids = entities.Map(x => x.Id);
            var success = await AsyncNative.DelAsync(ids.ToArray(), cancellationToken).IsSuccessAsync().ConfigureAwait(false);
            if (success) await client.RemoveTypeIdsAsync(ids.ToArray(), cancellationToken).ConfigureAwait(false);
            return success;
        }

        ValueTask<long> IRedisTypedClientAsync<T>.IncrementValueAsync(string key, CancellationToken cancellationToken)
            => AsyncNative.IncrAsync(key, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.IncrementValueByAsync(string key, int count, CancellationToken cancellationToken)
            => AsyncNative.IncrByAsync(key, count, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.DecrementValueAsync(string key, CancellationToken cancellationToken)
            => AsyncNative.DecrAsync(key, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.DecrementValueByAsync(string key, int count, CancellationToken cancellationToken)
            => AsyncNative.DecrByAsync(key, count, cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.ExpireInAsync(object id, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            var key = client.UrnKey<T>(id);
            return AsyncClient.ExpireEntryInAsync(key, expiresIn, cancellationToken);
        }

        ValueTask<bool> IRedisTypedClientAsync<T>.ExpireAtAsync(object id, DateTime expireAt, CancellationToken cancellationToken)
        {
            var key = client.UrnKey<T>(id);
            return AsyncClient.ExpireEntryAtAsync(key, expireAt, cancellationToken);
        }

        ValueTask<bool> IRedisTypedClientAsync<T>.ExpireEntryInAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken)
            => AsyncClient.ExpireEntryInAsync(key, expireIn, cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.ExpireEntryAtAsync(string key, DateTime expireAt, CancellationToken cancellationToken)
            => AsyncClient.ExpireEntryAtAsync(key, expireAt, cancellationToken);

        async ValueTask<TimeSpan> IRedisTypedClientAsync<T>.GetTimeToLiveAsync(string key, CancellationToken cancellationToken)
            => TimeSpan.FromSeconds(await AsyncNative.TtlAsync(key, cancellationToken).ConfigureAwait(false));

        ValueTask IRedisTypedClientAsync<T>.ForegroundSaveAsync(CancellationToken cancellationToken)
            => AsyncClient.ForegroundSaveAsync(cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.BackgroundSaveAsync(CancellationToken cancellationToken)
            => AsyncClient.BackgroundSaveAsync(cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.FlushDbAsync(CancellationToken cancellationToken)
            => AsyncClient.FlushDbAsync(cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.FlushAllAsync(CancellationToken cancellationToken)
            => new ValueTask(AsyncClient.FlushAllAsync(cancellationToken));

        async ValueTask<T[]> IRedisTypedClientAsync<T>.SearchKeysAsync(string pattern, CancellationToken cancellationToken)
        {
            var strKeys = await AsyncClient.SearchKeysAsync(pattern, cancellationToken).ConfigureAwait(false);
            return SearchKeysParse(strKeys);
        }

        private ValueTask<List<T>> CreateList(ValueTask<byte[][]> pending)
        {
            return pending.IsCompletedSuccessfully ? CreateList(pending.Result).AsValueTaskResult() : Awaited(this, pending);
            static async ValueTask<List<T>> Awaited(RedisTypedClient<T> obj, ValueTask<byte[][]> pending)
                => obj.CreateList(await pending.ConfigureAwait(false));
        }
        private ValueTask<T> DeserializeValueAsync(ValueTask<byte[]> pending)
        {
            return pending.IsCompletedSuccessfully ? DeserializeValue(pending.Result).AsValueTaskResult() : Awaited(this, pending);
            static async ValueTask<T> Awaited(RedisTypedClient<T> obj, ValueTask<byte[]> pending)
                => obj.DeserializeValue(await pending.ConfigureAwait(false));
        }

        private static ValueTask<T> DeserializeFromStringAsync(ValueTask<string> pending)
        {
            return pending.IsCompletedSuccessfully ? DeserializeFromString(pending.Result).AsValueTaskResult() : Awaited(pending);
            static async ValueTask<T> Awaited(ValueTask<string> pending)
                => DeserializeFromString(await pending.ConfigureAwait(false));
        }

        private static ValueTask<IDictionary<T, double>> CreateGenericMapAsync(ValueTask<IDictionary<string, double>> pending)
        {
            return pending.IsCompletedSuccessfully ? CreateGenericMap(pending.Result).AsValueTaskResult() : Awaited(pending);
            static async ValueTask<IDictionary<T, double>> Awaited(ValueTask<IDictionary<string, double>> pending)
                => CreateGenericMap(await pending.ConfigureAwait(false));
        }

        private static ValueTask<Dictionary<TKey, TValue>> ConvertEachToAsync<TKey, TValue>(ValueTask<Dictionary<string, string>> pending)
        {
            return pending.IsCompletedSuccessfully ? ConvertEachTo<TKey, TValue>(pending.Result).AsValueTaskResult() : Awaited(pending);
            static async ValueTask<Dictionary<TKey, TValue>> Awaited(ValueTask<Dictionary<string, string>> pending)
                => ConvertEachTo<TKey, TValue>(await pending.ConfigureAwait(false));
        }

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetSortedEntryValuesAsync(IRedisSetAsync<T> fromSet, int startingFrom, int endingAt, CancellationToken cancellationToken)
        {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, };
            var multiDataList = AsyncNative.SortAsync(fromSet.Id, sortOptions, cancellationToken);
            return CreateList(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.StoreAsHashAsync(T entity, CancellationToken cancellationToken)
            => AsyncClient.StoreAsHashAsync(entity, cancellationToken);

        ValueTask<T> IRedisTypedClientAsync<T>.GetFromHashAsync(object id, CancellationToken cancellationToken)
            => AsyncClient.GetFromHashAsync<T>(id, cancellationToken);

        async ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetAllItemsFromSetAsync(IRedisSetAsync<T> fromSet, CancellationToken cancellationToken)
        {
            var multiDataList = await AsyncNative.SMembersAsync(fromSet.Id, cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.AddItemToSetAsync(IRedisSetAsync<T> toSet, T item, CancellationToken cancellationToken)
            => AsyncNative.SAddAsync(toSet.Id, SerializeValue(item), cancellationToken).Await();

        ValueTask IRedisTypedClientAsync<T>.RemoveItemFromSetAsync(IRedisSetAsync<T> fromSet, T item, CancellationToken cancellationToken)
            => AsyncNative.SRemAsync(fromSet.Id, SerializeValue(item), cancellationToken).Await();

        ValueTask<T> IRedisTypedClientAsync<T>.PopItemFromSetAsync(IRedisSetAsync<T> fromSet, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.SPopAsync(fromSet.Id, cancellationToken));

        ValueTask IRedisTypedClientAsync<T>.MoveBetweenSetsAsync(IRedisSetAsync<T> fromSet, IRedisSetAsync<T> toSet, T item, CancellationToken cancellationToken)
            => AsyncNative.SMoveAsync(fromSet.Id, toSet.Id, SerializeValue(item), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetSetCountAsync(IRedisSetAsync<T> set, CancellationToken cancellationToken)
            => AsyncNative.SCardAsync(set.Id, cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.SetContainsItemAsync(IRedisSetAsync<T> set, T item, CancellationToken cancellationToken)
            => AsyncNative.SIsMemberAsync(set.Id, SerializeValue(item)).IsSuccessAsync();

        async ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetIntersectFromSetsAsync(IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
        {
            var multiDataList = await AsyncNative.SInterAsync(sets.Map(x => x.Id).ToArray(), cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.StoreIntersectFromSetsAsync(IRedisSetAsync<T> intoSet, IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
            => AsyncNative.SInterStoreAsync(intoSet.Id, sets.Map(x => x.Id).ToArray(), cancellationToken);

        async ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetUnionFromSetsAsync(IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
        {
            var multiDataList = await AsyncNative.SUnionAsync(sets.Map(x => x.Id).ToArray(), cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.StoreUnionFromSetsAsync(IRedisSetAsync<T> intoSet, IRedisSetAsync<T>[] sets, CancellationToken cancellationToken)
            => AsyncNative.SUnionStoreAsync(intoSet.Id, sets.Map(x => x.Id).ToArray(), cancellationToken);

        async ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetDifferencesFromSetAsync(IRedisSetAsync<T> fromSet, IRedisSetAsync<T>[] withSets, CancellationToken cancellationToken)
        {
            var multiDataList = await AsyncNative.SDiffAsync(fromSet.Id, withSets.Map(x => x.Id).ToArray(), cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.StoreDifferencesFromSetAsync(IRedisSetAsync<T> intoSet, IRedisSetAsync<T> fromSet, IRedisSetAsync<T>[] withSets, CancellationToken cancellationToken)
            => AsyncNative.SDiffStoreAsync(intoSet.Id, fromSet.Id, withSets.Map(x => x.Id).ToArray(), cancellationToken);

        ValueTask<T> IRedisTypedClientAsync<T>.GetRandomItemFromSetAsync(IRedisSetAsync<T> fromSet, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.SRandMemberAsync(fromSet.Id, cancellationToken));

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetAllItemsFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
        {
            var multiDataList = AsyncNative.LRangeAsync(fromList.Id, FirstElement, LastElement, cancellationToken);
            return CreateList(multiDataList);
        }

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromListAsync(IRedisListAsync<T> fromList, int startingFrom, int endingAt, CancellationToken cancellationToken)
        {
            var multiDataList = AsyncNative.LRangeAsync(fromList.Id, startingFrom, endingAt, cancellationToken);
            return CreateList(multiDataList);
        }

        ValueTask<List<T>> IRedisTypedClientAsync<T>.SortListAsync(IRedisListAsync<T> fromList, int startingFrom, int endingAt, CancellationToken cancellationToken)
        {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, };
            var multiDataList = AsyncNative.SortAsync(fromList.Id, sortOptions, cancellationToken);
            return CreateList(multiDataList);
        }

        ValueTask IRedisTypedClientAsync<T>.AddItemToListAsync(IRedisListAsync<T> fromList, T value, CancellationToken cancellationToken)
            => AsyncNative.RPushAsync(fromList.Id, SerializeValue(value), cancellationToken).Await();

        ValueTask IRedisTypedClientAsync<T>.PrependItemToListAsync(IRedisListAsync<T> fromList, T value, CancellationToken cancellationToken)
            => AsyncNative.LPushAsync(fromList.Id, SerializeValue(value), cancellationToken).Await();

        ValueTask<T> IRedisTypedClientAsync<T>.RemoveStartFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.LPopAsync(fromList.Id, cancellationToken));

        async ValueTask<T> IRedisTypedClientAsync<T>.BlockingRemoveStartFromListAsync(IRedisListAsync<T> fromList, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var unblockingKeyAndValue = await AsyncNative.BLPopAsync(fromList.Id, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            return unblockingKeyAndValue.Length == 0
                ? default
                : DeserializeValue(unblockingKeyAndValue[1]);
        }

        ValueTask<T> IRedisTypedClientAsync<T>.RemoveEndFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.RPopAsync(fromList.Id, cancellationToken));

        ValueTask IRedisTypedClientAsync<T>.RemoveAllFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => AsyncNative.LTrimAsync(fromList.Id, int.MaxValue, FirstElement, cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.TrimListAsync(IRedisListAsync<T> fromList, int keepStartingFrom, int keepEndingAt, CancellationToken cancellationToken)
            => AsyncNative.LTrimAsync(fromList.Id, keepStartingFrom, keepEndingAt, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.RemoveItemFromListAsync(IRedisListAsync<T> fromList, T value, CancellationToken cancellationToken)
        {
            const int removeAll = 0;
            return AsyncNative.LRemAsync(fromList.Id, removeAll, SerializeValue(value), cancellationToken);
        }

        ValueTask<long> IRedisTypedClientAsync<T>.RemoveItemFromListAsync(IRedisListAsync<T> fromList, T value, int noOfMatches, CancellationToken cancellationToken)
            => AsyncNative.LRemAsync(fromList.Id, noOfMatches, SerializeValue(value), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetListCountAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => AsyncNative.LLenAsync(fromList.Id, cancellationToken);

        ValueTask<T> IRedisTypedClientAsync<T>.GetItemFromListAsync(IRedisListAsync<T> fromList, int listIndex, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.LIndexAsync(fromList.Id, listIndex, cancellationToken));

        ValueTask IRedisTypedClientAsync<T>.SetItemInListAsync(IRedisListAsync<T> toList, int listIndex, T value, CancellationToken cancellationToken)
            => AsyncNative.LSetAsync(toList.Id, listIndex, SerializeValue(value), cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.InsertBeforeItemInListAsync(IRedisListAsync<T> toList, T pivot, T value, CancellationToken cancellationToken)
            => AsyncNative.LInsertAsync(toList.Id, insertBefore: true, pivot: SerializeValue(pivot), value: SerializeValue(value), cancellationToken: cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.InsertAfterItemInListAsync(IRedisListAsync<T> toList, T pivot, T value, CancellationToken cancellationToken)
            => AsyncNative.LInsertAsync(toList.Id, insertBefore: false, pivot: SerializeValue(pivot), value: SerializeValue(value), cancellationToken: cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.EnqueueItemOnListAsync(IRedisListAsync<T> fromList, T item, CancellationToken cancellationToken)
            => AsyncNative.LPushAsync(fromList.Id, SerializeValue(item), cancellationToken).Await();

        ValueTask<T> IRedisTypedClientAsync<T>.DequeueItemFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.RPopAsync(fromList.Id, cancellationToken));

        async ValueTask<T> IRedisTypedClientAsync<T>.BlockingDequeueItemFromListAsync(IRedisListAsync<T> fromList, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var unblockingKeyAndValue = await AsyncNative.BRPopAsync(fromList.Id, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            return unblockingKeyAndValue.Length == 0
                ? default
                : DeserializeValue(unblockingKeyAndValue[1]);
        }

        ValueTask IRedisTypedClientAsync<T>.PushItemToListAsync(IRedisListAsync<T> fromList, T item, CancellationToken cancellationToken)
            => AsyncNative.RPushAsync(fromList.Id, SerializeValue(item), cancellationToken).Await();

        ValueTask<T> IRedisTypedClientAsync<T>.PopItemFromListAsync(IRedisListAsync<T> fromList, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.RPopAsync(fromList.Id, cancellationToken));

        async ValueTask<T> IRedisTypedClientAsync<T>.BlockingPopItemFromListAsync(IRedisListAsync<T> fromList, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var unblockingKeyAndValue = await AsyncNative.BRPopAsync(fromList.Id, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            return unblockingKeyAndValue.Length == 0
                ? default
                : DeserializeValue(unblockingKeyAndValue[1]);
        }

        ValueTask<T> IRedisTypedClientAsync<T>.PopAndPushItemBetweenListsAsync(IRedisListAsync<T> fromList, IRedisListAsync<T> toList, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.RPopLPushAsync(fromList.Id, toList.Id, cancellationToken));

        ValueTask<T> IRedisTypedClientAsync<T>.BlockingPopAndPushItemBetweenListsAsync(IRedisListAsync<T> fromList, IRedisListAsync<T> toList, TimeSpan? timeOut, CancellationToken cancellationToken)
            => DeserializeValueAsync(AsyncNative.BRPopLPushAsync(fromList.Id, toList.Id, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken));

        ValueTask IRedisTypedClientAsync<T>.AddItemToSortedSetAsync(IRedisSortedSetAsync<T> toSet, T value, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSortedSetAsync(toSet.Id, value.SerializeToString(), cancellationToken).Await();

        ValueTask IRedisTypedClientAsync<T>.AddItemToSortedSetAsync(IRedisSortedSetAsync<T> toSet, T value, double score, CancellationToken cancellationToken)
            => AsyncClient.AddItemToSortedSetAsync(toSet.Id, value.SerializeToString(), score, cancellationToken).Await();

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveItemFromSortedSetAsync(IRedisSortedSetAsync<T> fromSet, T value, CancellationToken cancellationToken)
            => AsyncClient.RemoveItemFromSortedSetAsync(fromSet.Id, value.SerializeToString(), cancellationToken);

        ValueTask<T> IRedisTypedClientAsync<T>.PopItemWithLowestScoreFromSortedSetAsync(IRedisSortedSetAsync<T> fromSet, CancellationToken cancellationToken)
            => DeserializeFromStringAsync(AsyncClient.PopItemWithLowestScoreFromSortedSetAsync(fromSet.Id, cancellationToken));

        ValueTask<T> IRedisTypedClientAsync<T>.PopItemWithHighestScoreFromSortedSetAsync(IRedisSortedSetAsync<T> fromSet, CancellationToken cancellationToken)
            => DeserializeFromStringAsync(AsyncClient.PopItemWithHighestScoreFromSortedSetAsync(fromSet.Id, cancellationToken));

        ValueTask<bool> IRedisTypedClientAsync<T>.SortedSetContainsItemAsync(IRedisSortedSetAsync<T> set, T value, CancellationToken cancellationToken)
            => AsyncClient.SortedSetContainsItemAsync(set.Id, value.SerializeToString(), cancellationToken);

        ValueTask<double> IRedisTypedClientAsync<T>.IncrementItemInSortedSetAsync(IRedisSortedSetAsync<T> set, T value, double incrementBy, CancellationToken cancellationToken)
            => AsyncClient.IncrementItemInSortedSetAsync(set.Id, value.SerializeToString(), incrementBy, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetItemIndexInSortedSetAsync(IRedisSortedSetAsync<T> set, T value, CancellationToken cancellationToken)
            => AsyncClient.GetItemIndexInSortedSetAsync(set.Id, value.SerializeToString(), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetItemIndexInSortedSetDescAsync(IRedisSortedSetAsync<T> set, T value, CancellationToken cancellationToken)
            => AsyncClient.GetItemIndexInSortedSetDescAsync(set.Id, value.SerializeToString(), cancellationToken);

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetAllItemsFromSortedSetAsync(IRedisSortedSetAsync<T> set, CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSortedSetAsync(set.Id, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetAllItemsFromSortedSetDescAsync(IRedisSortedSetAsync<T> set, CancellationToken cancellationToken)
            => AsyncClient.GetAllItemsFromSortedSetDescAsync(set.Id, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetAsync(IRedisSortedSetAsync<T> set, int fromRank, int toRank, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetAsync(set.Id, fromRank, toRank, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetDescAsync(IRedisSortedSetAsync<T> set, int fromRank, int toRank, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetDescAsync(set.Id, fromRank, toRank, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetAllWithScoresFromSortedSetAsync(IRedisSortedSetAsync<T> set, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetAsync(set.Id, FirstElement, LastElement, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetAsync(IRedisSortedSetAsync<T> set, int fromRank, int toRank, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetAsync(set.Id, fromRank, toRank, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetDescAsync(IRedisSortedSetAsync<T> set, int fromRank, int toRank, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetDescAsync(set.Id, fromRank, toRank, cancellationToken));

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(set.Id, fromStringScore, toStringScore, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(set.Id, fromStringScore, toStringScore, skip, take, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(set.Id, fromScore, toScore, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByLowestScoreAsync(set.Id, fromScore, toScore, skip, take, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByLowestScoreAsync(set.Id, fromStringScore, toStringScore, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByLowestScoreAsync(set.Id, fromStringScore, toStringScore, skip, take, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByLowestScoreAsync(set.Id, fromScore, toScore, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByLowestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByLowestScoreAsync(set.Id, fromScore, toScore, skip, take, cancellationToken));
        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(set.Id, fromStringScore, toStringScore, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(set.Id, fromStringScore, toStringScore, skip, take, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(set.Id, fromScore, toScore, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetRangeFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => AsyncClient.GetRangeFromSortedSetByHighestScoreAsync(set.Id, fromScore, toScore, skip, take, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByHighestScoreAsync(set.Id, fromStringScore, toStringScore, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByHighestScoreAsync(set.Id, fromStringScore, toStringScore, skip, take, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByHighestScoreAsync(set.Id, fromScore, toScore, cancellationToken));

        ValueTask<IDictionary<T, double>> IRedisTypedClientAsync<T>.GetRangeWithScoresFromSortedSetByHighestScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateGenericMapAsync(AsyncClient.GetRangeWithScoresFromSortedSetByHighestScoreAsync(set.Id, fromScore, toScore, skip, take, cancellationToken));

        ValueTask<long> IRedisTypedClientAsync<T>.RemoveRangeFromSortedSetAsync(IRedisSortedSetAsync<T> set, int minRank, int maxRank, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetAsync(set.Id, minRank, maxRank, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.RemoveRangeFromSortedSetByScoreAsync(IRedisSortedSetAsync<T> set, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsyncClient.RemoveRangeFromSortedSetByScoreAsync(set.Id, fromScore, toScore, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetSortedSetCountAsync(IRedisSortedSetAsync<T> set, CancellationToken cancellationToken)
            => AsyncClient.GetSortedSetCountAsync(set.Id, cancellationToken);

        ValueTask<double> IRedisTypedClientAsync<T>.GetItemScoreInSortedSetAsync(IRedisSortedSetAsync<T> set, T value, CancellationToken cancellationToken)
            => AsyncClient.GetItemScoreInSortedSetAsync(set.Id, value.SerializeToString(), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreIntersectFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, IRedisSortedSetAsync<T>[] setIds, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSortedSetsAsync(intoSetId.Id, setIds.Map(x => x.Id).ToArray(), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreIntersectFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, IRedisSortedSetAsync<T>[] setIds, string[] args, CancellationToken cancellationToken)
            => AsyncClient.StoreIntersectFromSortedSetsAsync(intoSetId.Id, setIds.Map(x => x.Id).ToArray(), args, cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreUnionFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, IRedisSortedSetAsync<T>[] setIds, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSortedSetsAsync(intoSetId.Id, setIds.Map(x => x.Id).ToArray(), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreUnionFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, IRedisSortedSetAsync<T>[] setIds, string[] args, CancellationToken cancellationToken)
            => AsyncClient.StoreUnionFromSortedSetsAsync(intoSetId.Id, setIds.Map(x => x.Id).ToArray(), args, cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.HashContainsEntryAsync<TKey>(IRedisHashAsync<TKey, T> hash, TKey key, CancellationToken cancellationToken)
            => AsyncClient.HashContainsEntryAsync(hash.Id, key.SerializeToString(), cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.SetEntryInHashAsync<TKey>(IRedisHashAsync<TKey, T> hash, TKey key, T value, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashAsync(hash.Id, key.SerializeToString(), value.SerializeToString(), cancellationToken);

        ValueTask<bool> IRedisTypedClientAsync<T>.SetEntryInHashIfNotExistsAsync<TKey>(IRedisHashAsync<TKey, T> hash, TKey key, T value, CancellationToken cancellationToken)
            => AsyncClient.SetEntryInHashIfNotExistsAsync(hash.Id, key.SerializeToString(), value.SerializeToString(), cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.SetRangeInHashAsync<TKey>(IRedisHashAsync<TKey, T> hash, IEnumerable<KeyValuePair<TKey, T>> keyValuePairs, CancellationToken cancellationToken)
        {
            var stringKeyValuePairs = keyValuePairs.ToList().ConvertAll(
                x => new KeyValuePair<string, string>(x.Key.SerializeToString(), x.Value.SerializeToString()));

            return AsyncClient.SetRangeInHashAsync(hash.Id, stringKeyValuePairs, cancellationToken);
        }

        ValueTask<T> IRedisTypedClientAsync<T>.GetValueFromHashAsync<TKey>(IRedisHashAsync<TKey, T> hash, TKey key, CancellationToken cancellationToken)
            => DeserializeFromStringAsync(AsyncClient.GetValueFromHashAsync(hash.Id, key.SerializeToString(), cancellationToken));

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryFromHashAsync<TKey>(IRedisHashAsync<TKey, T> hash, TKey key, CancellationToken cancellationToken)
            => AsyncClient.RemoveEntryFromHashAsync(hash.Id, key.SerializeToString(), cancellationToken);

        ValueTask<long> IRedisTypedClientAsync<T>.GetHashCountAsync<TKey>(IRedisHashAsync<TKey, T> hash, CancellationToken cancellationToken)
            => AsyncClient.GetHashCountAsync(hash.Id, cancellationToken);

        ValueTask<List<TKey>> IRedisTypedClientAsync<T>.GetHashKeysAsync<TKey>(IRedisHashAsync<TKey, T> hash, CancellationToken cancellationToken)
            => AsyncClient.GetHashKeysAsync(hash.Id, cancellationToken).ConvertEachToAsync<TKey>();

        ValueTask<List<T>> IRedisTypedClientAsync<T>.GetHashValuesAsync<TKey>(IRedisHashAsync<TKey, T> hash, CancellationToken cancellationToken)
            => AsyncClient.GetHashValuesAsync(hash.Id, cancellationToken).ConvertEachToAsync<T>();

        ValueTask<Dictionary<TKey, T>> IRedisTypedClientAsync<T>.GetAllEntriesFromHashAsync<TKey>(IRedisHashAsync<TKey, T> hash, CancellationToken cancellationToken)
            => ConvertEachToAsync<TKey, T>(AsyncClient.GetAllEntriesFromHashAsync(hash.Id, cancellationToken));

        async ValueTask IRedisTypedClientAsync<T>.StoreRelatedEntitiesAsync<TChild>(object parentId, List<TChild> children, CancellationToken cancellationToken)
        {
            var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
            var childKeys = children.ConvertAll(x => client.UrnKey(x));

            await using var trans = await AsyncClient.CreateTransactionAsync(cancellationToken).ConfigureAwait(false);
            //Ugly but need access to a generic constraint-free StoreAll method
            trans.QueueCommand(c => ((RedisClient)c).StoreAllAsyncImpl(children, cancellationToken));
            trans.QueueCommand(c => c.AddRangeToSetAsync(childRefKey, childKeys, cancellationToken));

            await trans.CommitAsync(cancellationToken).ConfigureAwait(false);
        }

        ValueTask IRedisTypedClientAsync<T>.StoreRelatedEntitiesAsync<TChild>(object parentId, TChild[] children, CancellationToken cancellationToken)
            => AsAsync().StoreRelatedEntitiesAsync(parentId, new List<TChild>(children), cancellationToken);

        ValueTask IRedisTypedClientAsync<T>.DeleteRelatedEntitiesAsync<TChild>(object parentId, CancellationToken cancellationToken)
        {
            var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
            return new ValueTask(AsyncClient.RemoveAsync(childRefKey, cancellationToken));
        }

        ValueTask IRedisTypedClientAsync<T>.DeleteRelatedEntityAsync<TChild>(object parentId, object childId, CancellationToken cancellationToken)
        {
            var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
            return AsyncClient.RemoveItemFromSetAsync(childRefKey, TypeSerializer.SerializeToString(childId), cancellationToken);
        }

        async ValueTask<List<TChild>> IRedisTypedClientAsync<T>.GetRelatedEntitiesAsync<TChild>(object parentId, CancellationToken cancellationToken)
        {
            var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
            var childKeys = (await AsyncClient.GetAllItemsFromSetAsync(childRefKey, cancellationToken).ConfigureAwait(false)).ToList();

            return await AsyncClient.As<TChild>().GetValuesAsync(childKeys, cancellationToken).ConfigureAwait(false);
        }

        ValueTask<long> IRedisTypedClientAsync<T>.GetRelatedEntitiesCountAsync<TChild>(object parentId, CancellationToken cancellationToken)
        {
            var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
            return AsyncClient.GetSetCountAsync(childRefKey, cancellationToken);
        }

        ValueTask IRedisTypedClientAsync<T>.AddToRecentsListAsync(T value, CancellationToken cancellationToken)
        {
            var key = client.UrnKey(value);
            var nowScore = DateTime.UtcNow.ToUnixTime();
            return AsyncClient.AddItemToSortedSetAsync(RecentSortedSetKey, key, nowScore, cancellationToken).Await();
        }

        async ValueTask<List<T>> IRedisTypedClientAsync<T>.GetLatestFromRecentsListAsync(int skip, int take, CancellationToken cancellationToken)
        {
            var toRank = take - 1;
            var keys = await AsyncClient.GetRangeFromSortedSetDescAsync(RecentSortedSetKey, skip, toRank, cancellationToken).ConfigureAwait(false);
            var values = await AsAsync().GetValuesAsync(keys, cancellationToken).ConfigureAwait(false);
            return values;
        }

        async ValueTask<List<T>> IRedisTypedClientAsync<T>.GetEarliestFromRecentsListAsync(int skip, int take, CancellationToken cancellationToken)
        {
            var toRank = take - 1;
            var keys = await AsyncClient.GetRangeFromSortedSetAsync(RecentSortedSetKey, skip, toRank, cancellationToken).ConfigureAwait(false);
            var values = await AsAsync().GetValuesAsync(keys, cancellationToken).ConfigureAwait(false);
            return values;
        }

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryAsync(params string[] args)
            => AsAsync().RemoveEntryAsync(args, cancellationToken: default);

        ValueTask<bool> IRedisTypedClientAsync<T>.RemoveEntryAsync(params IHasStringId[] entities)
            => AsAsync().RemoveEntryAsync(entities, cancellationToken: default);

        ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetIntersectFromSetsAsync(params IRedisSetAsync<T>[] sets)
            => AsAsync().GetIntersectFromSetsAsync(sets, cancellationToken: default);

        ValueTask IRedisTypedClientAsync<T>.StoreIntersectFromSetsAsync(IRedisSetAsync<T> intoSet, params IRedisSetAsync<T>[] sets)
            => AsAsync().StoreIntersectFromSetsAsync(intoSet, sets, cancellationToken: default);

        ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetUnionFromSetsAsync(params IRedisSetAsync<T>[] sets)
            => AsAsync().GetUnionFromSetsAsync(sets, cancellationToken: default);

        ValueTask IRedisTypedClientAsync<T>.StoreUnionFromSetsAsync(IRedisSetAsync<T> intoSet, params IRedisSetAsync<T>[] sets)
            => AsAsync().StoreUnionFromSetsAsync(intoSet, sets, cancellationToken: default);

        ValueTask<HashSet<T>> IRedisTypedClientAsync<T>.GetDifferencesFromSetAsync(IRedisSetAsync<T> fromSet, params IRedisSetAsync<T>[] withSets)
            => AsAsync().GetDifferencesFromSetAsync(fromSet, withSets, cancellationToken: default);

        ValueTask IRedisTypedClientAsync<T>.StoreDifferencesFromSetAsync(IRedisSetAsync<T> intoSet, IRedisSetAsync<T> fromSet, params IRedisSetAsync<T>[] withSets)
            => AsAsync().StoreDifferencesFromSetAsync(intoSet, fromSet, withSets, cancellationToken: default);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreIntersectFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, params IRedisSortedSetAsync<T>[] setIds)
            => AsAsync().StoreIntersectFromSortedSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask<long> IRedisTypedClientAsync<T>.StoreUnionFromSortedSetsAsync(IRedisSortedSetAsync<T> intoSetId, params IRedisSortedSetAsync<T>[] setIds)
            => AsAsync().StoreUnionFromSortedSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask IRedisTypedClientAsync<T>.StoreRelatedEntitiesAsync<TChild>(object parentId, params TChild[] children)
            => AsAsync().StoreRelatedEntitiesAsync<TChild>(parentId, children, cancellationToken: default);
    }
}