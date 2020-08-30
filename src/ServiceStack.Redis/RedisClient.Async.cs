//
// https://github.com/ServiceStack/ServiceStack.Redis/
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
using ServiceStack.Data;
using ServiceStack.Model;
using ServiceStack.Redis.Generic;
using ServiceStack.Redis.Internal;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Text;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis
{
    partial class RedisClient : IRedisClientAsync, IRemoveByPatternAsync, ICacheClientAsync
    {
        /// <summary>
        /// Access this instance for async usage
        /// </summary>
        public IRedisClientAsync AsAsync() => this;

        // the typed client implements this for us
        IRedisTypedClientAsync<T> IRedisClientAsync.As<T>() => (IRedisTypedClientAsync<T>)As<T>();

        // convenience since we're not saturating the public API; this makes it easy to call
        // the explicit interface implementations; the JIT should make this a direct call
        private IRedisNativeClientAsync NativeAsync => this;

        IHasNamed<IRedisListAsync> IRedisClientAsync.Lists => Lists as IHasNamed<IRedisListAsync> ?? throw new NotSupportedException($"The provided Lists ({Lists?.GetType().FullName}) does not support IRedisListAsync");
        IHasNamed<IRedisSetAsync> IRedisClientAsync.Sets => Sets as IHasNamed<IRedisSetAsync> ?? throw new NotSupportedException($"The provided Sets ({Sets?.GetType().FullName})does not support IRedisSetAsync");
        IHasNamed<IRedisSortedSetAsync> IRedisClientAsync.SortedSets => SortedSets as IHasNamed<IRedisSortedSetAsync> ?? throw new NotSupportedException($"The provided SortedSets ({SortedSets?.GetType().FullName})does not support IRedisSortedSetAsync");
        IHasNamed<IRedisHashAsync> IRedisClientAsync.Hashes => Hashes as IHasNamed<IRedisHashAsync> ?? throw new NotSupportedException($"The provided Hashes ({Hashes?.GetType().FullName})does not support IRedisHashAsync");

        internal ValueTask RegisterTypeIdAsync<T>(T value, CancellationToken cancellationToken)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            var id = value.GetId().ToString();

            return RegisterTypeIdAsync(typeIdsSetKey, id, cancellationToken);
        }
        internal ValueTask RegisterTypeIdAsync(string typeIdsSetKey, string id, CancellationToken cancellationToken)
        {
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                registeredTypeIdsWithinPipeline.Add(id);
                return default;
            }
            else
            {
                return AsAsync().AddItemToSetAsync(typeIdsSetKey, id, cancellationToken);
            }
        }

        // Called just after original Pipeline is closed.
        internal async ValueTask AddTypeIdsRegisteredDuringPipelineAsync(CancellationToken cancellationToken)
        {
            foreach (var entry in registeredTypeIdsWithinPipelineMap)
            {
                await AsAsync().AddRangeToSetAsync(entry.Key, entry.Value.ToList(), cancellationToken).ConfigureAwait(false);
            }
            registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();
        }


        ValueTask<DateTime> IRedisClientAsync.GetServerTimeAsync(CancellationToken cancellationToken)
            => NativeAsync.TimeAsync(cancellationToken).Await(parts => ParseTimeResult(parts));

        IRedisPipelineAsync IRedisClientAsync.CreatePipeline()
            => new RedisAllPurposePipeline(this);

        ValueTask<IRedisTransactionAsync> IRedisClientAsync.CreateTransactionAsync(CancellationToken cancellationToken)
        {
            AssertServerVersionNumber(); // pre-fetch call to INFO before transaction if needed
            return new RedisTransaction(this, true).AsValueTask<IRedisTransactionAsync>(); // note that the MULTI here will be held and flushed async
        }

        ValueTask<bool> IRedisClientAsync.RemoveEntryAsync(string[] keys, CancellationToken cancellationToken)
            => keys.Length == 0 ? default : NativeAsync.DelAsync(keys, cancellationToken).IsSuccessAsync();

        private async ValueTask ExecAsync(Func<IRedisClientAsync, ValueTask> action)
        {
            using (JsConfig.With(new Text.Config { ExcludeTypeInfo = false }))
            {
                await action(this).ConfigureAwait(false);
            }
        }

        private async ValueTask<T> ExecAsync<T>(Func<IRedisClientAsync, ValueTask<T>> action)
        {
            using (JsConfig.With(new Text.Config { ExcludeTypeInfo = false }))
            {
                return await action(this).ConfigureAwait(false);
            }
        }

        ValueTask IRedisClientAsync.SetValueAsync(string key, string value, CancellationToken cancellationToken)
        {
            var bytesValue = value?.ToUtf8Bytes();
            return NativeAsync.SetAsync(key, bytesValue, cancellationToken: cancellationToken);
        }

        ValueTask<string> IRedisClientAsync.GetValueAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.GetAsync(key, cancellationToken).FromUtf8BytesAsync();

        ValueTask<T> ICacheClientAsync.GetAsync<T>(string key, CancellationToken cancellationToken)
        {
            return ExecAsync(r =>
                typeof(T) == typeof(byte[])
                    ? ((IRedisNativeClientAsync)r).GetAsync(key, cancellationToken).Await(val => (T)(object)val)
                    : r.GetValueAsync(key, cancellationToken).Await(val => JsonSerializer.DeserializeFromString<T>(val))
            );
        }

        async ValueTask<List<string>> IRedisClientAsync.SearchKeysAsync(string pattern, CancellationToken cancellationToken)
        {
            var list = new List<string>();
            await foreach (var value in ((IRedisClientAsync)this).ScanAllKeysAsync(pattern, cancellationToken: cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                list.Add(value);
            }
            return list;
        }

        async IAsyncEnumerable<string> IRedisClientAsync.ScanAllKeysAsync(string pattern, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            ScanResult ret = default;
            while (true)
            {
                ret = await (pattern != null // note ConfigureAwait is handled below
                    ? NativeAsync.ScanAsync(ret?.Cursor ?? 0, pageSize, match: pattern, cancellationToken: cancellationToken)
                    : NativeAsync.ScanAsync(ret?.Cursor ?? 0, pageSize, cancellationToken: cancellationToken)
                    ).ConfigureAwait(false);

                foreach (var key in ret.Results)
                {
                    yield return key.FromUtf8Bytes();
                }

                if (ret.Cursor == 0) break;
            }
        }

        ValueTask<RedisKeyType> IRedisClientAsync.GetEntryTypeAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.TypeAsync(key, cancellationToken).Await((val, state) => state.ParseEntryType(val), this);

        ValueTask IRedisClientAsync.AddItemToSetAsync(string setId, string item, CancellationToken cancellationToken)
            => NativeAsync.SAddAsync(setId, item.ToUtf8Bytes(), cancellationToken).Await();

        ValueTask IRedisClientAsync.AddItemToListAsync(string listId, string value, CancellationToken cancellationToken)
            => NativeAsync.RPushAsync(listId, value.ToUtf8Bytes(), cancellationToken).Await();

        ValueTask<bool> IRedisClientAsync.AddItemToSortedSetAsync(string setId, string value, CancellationToken cancellationToken)
            => ((IRedisClientAsync)this).AddItemToSortedSetAsync(setId, value, GetLexicalScore(value), cancellationToken);

        ValueTask<bool> IRedisClientAsync.AddItemToSortedSetAsync(string setId, string value, double score, CancellationToken cancellationToken)
            => NativeAsync.ZAddAsync(setId, score, value.ToUtf8Bytes()).IsSuccessAsync();

        ValueTask<bool> IRedisClientAsync.SetEntryInHashAsync(string hashId, string key, string value, CancellationToken cancellationToken)
            => NativeAsync.HSetAsync(hashId, key.ToUtf8Bytes(), value.ToUtf8Bytes()).IsSuccessAsync();

        ValueTask IRedisClientAsync.SetAllAsync(IDictionary<string, string> map, CancellationToken cancellationToken)
            => GetSetAllBytes(map, out var keyBytes, out var valBytes) ? NativeAsync.MSetAsync(keyBytes, valBytes, cancellationToken) : default;

        ValueTask IRedisClientAsync.SetAllAsync(IEnumerable<string> keys, IEnumerable<string> values, CancellationToken cancellationToken)
            => GetSetAllBytes(keys, values, out var keyBytes, out var valBytes) ? NativeAsync.MSetAsync(keyBytes, valBytes, cancellationToken) : default;

        ValueTask ICacheClientAsync.SetAllAsync<T>(IDictionary<string, T> values, CancellationToken cancellationToken)
        {
            if (values.Count != 0)
            {
                return ExecAsync(r =>
                {
                    // need to do this inside Exec for the JSON config bits
                    GetSetAllBytesTyped<T>(values, out var keys, out var valBytes);
                    return ((IRedisNativeClientAsync)r).MSetAsync(keys, valBytes, cancellationToken);
                });
            }
            else
            {
                return default;
            }
        }

        ValueTask IRedisClientAsync.RenameKeyAsync(string fromName, string toName, CancellationToken cancellationToken)
            => NativeAsync.RenameAsync(fromName, toName, cancellationToken);

        ValueTask<bool> IRedisClientAsync.ContainsKeyAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.ExistsAsync(key, cancellationToken).IsSuccessAsync();


        ValueTask<string> IRedisClientAsync.GetRandomKeyAsync(CancellationToken cancellationToken)
            => NativeAsync.RandomKeyAsync(cancellationToken);

        ValueTask IRedisClientAsync.SelectAsync(long db, CancellationToken cancellationToken)
            => NativeAsync.SelectAsync(db, cancellationToken);

        ValueTask<bool> IRedisClientAsync.ExpireEntryInAsync(string key, TimeSpan expireIn, CancellationToken cancellationToken)
            => UseMillisecondExpiration(expireIn)
            ? NativeAsync.PExpireAsync(key, (long)expireIn.TotalMilliseconds, cancellationToken)
            : NativeAsync.ExpireAsync(key, (int)expireIn.TotalSeconds, cancellationToken);

        ValueTask<bool> IRedisClientAsync.ExpireEntryAtAsync(string key, DateTime expireAt, CancellationToken cancellationToken)
            => AssertServerVersionNumber() >= 2600
            ? NativeAsync.PExpireAtAsync(key, ConvertToServerDate(expireAt).ToUnixTimeMs(), cancellationToken)
            : NativeAsync.ExpireAtAsync(key, ConvertToServerDate(expireAt).ToUnixTime(), cancellationToken);

        ValueTask<TimeSpan?> ICacheClientExtendedAsync.GetTimeToLiveAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.TtlAsync(key, cancellationToken).Await(ttlSecs => ParseTimeToLiveResult(ttlSecs));

        ValueTask<bool> IRedisClientAsync.PingAsync(CancellationToken cancellationToken)
            => NativeAsync.PingAsync(cancellationToken);

        ValueTask<string> IRedisClientAsync.EchoAsync(string text, CancellationToken cancellationToken)
            => NativeAsync.EchoAsync(text, cancellationToken);

        ValueTask IRedisClientAsync.ForegroundSaveAsync(CancellationToken cancellationToken)
            => NativeAsync.SaveAsync(cancellationToken);

        ValueTask IRedisClientAsync.BackgroundSaveAsync(CancellationToken cancellationToken)
            => NativeAsync.BgSaveAsync(cancellationToken);

        ValueTask IRedisClientAsync.ShutdownAsync(CancellationToken cancellationToken)
            => NativeAsync.ShutdownAsync(false, cancellationToken);

        ValueTask IRedisClientAsync.ShutdownNoSaveAsync(CancellationToken cancellationToken)
            => NativeAsync.ShutdownAsync(true, cancellationToken);

        ValueTask IRedisClientAsync.BackgroundRewriteAppendOnlyFileAsync(CancellationToken cancellationToken)
            => NativeAsync.BgRewriteAofAsync(cancellationToken);

        ValueTask IRedisClientAsync.FlushDbAsync(CancellationToken cancellationToken)
            => NativeAsync.FlushDbAsync(cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetValuesAsync(List<string> keys, CancellationToken cancellationToken)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (keys.Count == 0) return new List<string>().AsValueTask();

            return NativeAsync.MGetAsync(keys.ToArray(), cancellationToken).Await(val => ParseGetValuesResult(val));
        }

        ValueTask<List<T>> IRedisClientAsync.GetValuesAsync<T>(List<string> keys, CancellationToken cancellationToken)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (keys.Count == 0) return new List<T>().AsValueTask();

            return NativeAsync.MGetAsync(keys.ToArray(), cancellationToken).Await(value => ParseGetValuesResult<T>(value));
        }

        ValueTask<Dictionary<string, string>> IRedisClientAsync.GetValuesMapAsync(List<string> keys, CancellationToken cancellationToken)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (keys.Count == 0) return new Dictionary<string, string>().AsValueTask();

            var keysArray = keys.ToArray();
            return NativeAsync.MGetAsync(keysArray, cancellationToken).Await((resultBytesArray, state) => ParseGetValuesMapResult(state, resultBytesArray), keysArray);
        }

        ValueTask<Dictionary<string, T>> IRedisClientAsync.GetValuesMapAsync<T>(List<string> keys, CancellationToken cancellationToken)
        {
            if (keys == null) throw new ArgumentNullException(nameof(keys));
            if (keys.Count == 0) return new Dictionary<string, T>().AsValueTask();

            var keysArray = keys.ToArray();
            return NativeAsync.MGetAsync(keysArray, cancellationToken).Await((resultBytesArray, state) => ParseGetValuesMapResult<T>(state, resultBytesArray), keysArray);
        }

        ValueTask<IAsyncDisposable> IRedisClientAsync.AcquireLockAsync(string key, TimeSpan? timeOut, CancellationToken cancellationToken)
            => RedisLock.CreateAsync(this, key, timeOut, cancellationToken).Await<RedisLock, IAsyncDisposable>(value => value);

        ValueTask IRedisClientAsync.SetValueAsync(string key, string value, TimeSpan expireIn, CancellationToken cancellationToken)
        {
            var bytesValue = value?.ToUtf8Bytes();

            if (AssertServerVersionNumber() >= 2610)
            {
                PickTime(expireIn, out var seconds, out var milliseconds);
                return NativeAsync.SetAsync(key, bytesValue, expirySeconds: seconds,
                    expiryMilliseconds: milliseconds, cancellationToken: cancellationToken);
            }
            else
            {
                return NativeAsync.SetExAsync(key, (int)expireIn.TotalSeconds, bytesValue, cancellationToken);
            }
        }

        static void PickTime(TimeSpan? value, out long expirySeconds, out long expiryMilliseconds)
        {
            expirySeconds = expiryMilliseconds = 0;
            if (value.HasValue)
            {
                var expireIn = value.GetValueOrDefault();
                if (expireIn.Milliseconds > 0)
                {
                    expiryMilliseconds = (long)expireIn.TotalMilliseconds;
                }
                else
                {
                    expirySeconds = (long)expireIn.TotalSeconds;
                }
            }
        }
        ValueTask<bool> IRedisClientAsync.SetValueIfNotExistsAsync(string key, string value, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            var bytesValue = value?.ToUtf8Bytes();
            PickTime(expireIn, out var seconds, out var milliseconds);
            return NativeAsync.SetAsync(key, bytesValue, false, seconds, milliseconds, cancellationToken);
        }

        ValueTask<bool> IRedisClientAsync.SetValueIfExistsAsync(string key, string value, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            var bytesValue = value?.ToUtf8Bytes();
            PickTime(expireIn, out var seconds, out var milliseconds);
            return NativeAsync.SetAsync(key, bytesValue, true, seconds, milliseconds, cancellationToken);
        }

        ValueTask IRedisClientAsync.WatchAsync(string[] keys, CancellationToken cancellationToken)
            => NativeAsync.WatchAsync(keys, cancellationToken);

        ValueTask IRedisClientAsync.UnWatchAsync(CancellationToken cancellationToken)
            => NativeAsync.UnWatchAsync(cancellationToken);

        ValueTask<long> IRedisClientAsync.AppendToValueAsync(string key, string value, CancellationToken cancellationToken)
            => NativeAsync.AppendAsync(key, value.ToUtf8Bytes(), cancellationToken);

        async ValueTask<object> IRedisClientAsync.StoreObjectAsync(object entity, CancellationToken cancellationToken)
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));

            var id = entity.GetObjectId();
            var entityType = entity.GetType();
            var urnKey = UrnKey(entityType, id);
            var valueString = JsonSerializer.SerializeToString(entity);

            await ((IRedisClientAsync)this).SetValueAsync(urnKey, valueString, cancellationToken).ConfigureAwait(false);

            await RegisterTypeIdAsync(GetTypeIdsSetKey(entityType), id.ToString(), cancellationToken).ConfigureAwait(false);

            return entity;
        }

        ValueTask<string> IRedisClientAsync.PopItemFromSetAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.SPopAsync(setId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<List<string>> IRedisClientAsync.PopItemsFromSetAsync(string setId, int count, CancellationToken cancellationToken)
            => NativeAsync.SPopAsync(setId, count, cancellationToken).ToStringListAsync();

        ValueTask IRedisClientAsync.SlowlogResetAsync(CancellationToken cancellationToken)
            => NativeAsync.SlowlogResetAsync(cancellationToken);

        ValueTask<SlowlogItem[]> IRedisClientAsync.GetSlowlogAsync(int? numberOfRecords, CancellationToken cancellationToken)
            => NativeAsync.SlowlogGetAsync(numberOfRecords, cancellationToken).Await(data => ParseSlowlog(data));


        ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, CancellationToken cancellationToken)
            => ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), cancellationToken: cancellationToken)).AwaitAsTrue();

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose();
            return default;
        }

        ValueTask<long> IRedisClientAsync.GetSortedSetCountAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.ZCardAsync(setId, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetSortedSetCountAsync(string setId, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
        {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return AsAsync().GetSortedSetCountAsync(setId, fromScore, toScore, cancellationToken);
        }

        ValueTask<long> IRedisClientAsync.GetSortedSetCountAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => NativeAsync.ZCountAsync(setId, fromScore, toScore, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetSortedSetCountAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => NativeAsync.ZCountAsync(setId, fromScore, toScore, cancellationToken);

        ValueTask<double> IRedisClientAsync.GetItemScoreInSortedSetAsync(string setId, string value, CancellationToken cancellationToken)
            => NativeAsync.ZScoreAsync(setId, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<RedisText> IRedisClientAsync.CustomAsync(object[] cmdWithArgs, CancellationToken cancellationToken)
            => RawCommandAsync(cancellationToken, cmdWithArgs).Await(result => result.ToRedisText());

        ValueTask IRedisClientAsync.SetValuesAsync(IDictionary<string, string> map, CancellationToken cancellationToken)
            => ((IRedisClientAsync)this).SetAllAsync(map, cancellationToken);

        ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            AssertNotInTransaction();
            return ExecAsync(async r =>
            {
                await r.SetAsync(key, value).ConfigureAwait(false);
                await r.ExpireEntryAtAsync(key, ConvertToServerDate(expiresAt)).ConfigureAwait(false);
            }).AwaitAsTrue();
        }
        ValueTask<bool> ICacheClientAsync.SetAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
        {
            if (AssertServerVersionNumber() >= 2600)
            {
                return ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), 0, expiryMilliseconds: (long)expiresIn.TotalMilliseconds)).AwaitAsTrue();
            }
            else
            {
                return ExecAsync(r => ((IRedisNativeClientAsync)r).SetExAsync(key, (int)expiresIn.TotalSeconds, ToBytes(value))).AwaitAsTrue();
            }
        }

        ValueTask ICacheClientAsync.FlushAllAsync(CancellationToken cancellationToken)
            => NativeAsync.FlushAllAsync(cancellationToken);

        ValueTask<IDictionary<string, T>> ICacheClientAsync.GetAllAsync<T>(IEnumerable<string> keys, CancellationToken cancellationToken)
        {
            return ExecAsync(r =>
            {
                var keysArray = keys.ToArray();

                return ((IRedisNativeClientAsync)r).MGetAsync(keysArray, cancellationToken).Await((keyValues, state) => ProcessGetAllResult<T>(state, keyValues), keysArray);
            });
        }

        ValueTask<bool> ICacheClientAsync.RemoveAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.DelAsync(key, cancellationToken).IsSuccessAsync();

        IAsyncEnumerable<string> ICacheClientExtendedAsync.GetKeysByPatternAsync(string pattern, CancellationToken cancellationToken)
            => AsAsync().ScanAllKeysAsync(pattern, cancellationToken: cancellationToken);

        ValueTask ICacheClientExtendedAsync.RemoveExpiredEntriesAsync(CancellationToken cancellationToken)
        {
            //Redis automatically removed expired Cache Entries
            return default;
        }

        async ValueTask IRemoveByPatternAsync.RemoveByPatternAsync(string pattern, CancellationToken cancellationToken)
        {
            List<string> buffer = null;
            const int BATCH_SIZE = 1024;
            await foreach (var key in AsAsync().ScanAllKeysAsync(pattern, cancellationToken: cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                (buffer ??= new List<string>()).Add(key);
                if (buffer.Count == BATCH_SIZE)
                {
                    await NativeAsync.DelAsync(buffer.ToArray(), cancellationToken).ConfigureAwait(false);
                    buffer.Clear();
                }
            }
            if (buffer is object && buffer.Count != 0)
            {
                await NativeAsync.DelAsync(buffer.ToArray(), cancellationToken).ConfigureAwait(false);
            }
        }

        ValueTask IRemoveByPatternAsync.RemoveByRegexAsync(string regex, CancellationToken cancellationToken)
            => AsAsync().RemoveByPatternAsync(RegexToGlob(regex), cancellationToken);

        ValueTask ICacheClientAsync.RemoveAllAsync(IEnumerable<string> keys, CancellationToken cancellationToken)
            => ExecAsync(r => r.RemoveEntryAsync(keys.ToArray(), cancellationToken)).Await();

        ValueTask<long> ICacheClientAsync.IncrementAsync(string key, uint amount, CancellationToken cancellationToken)
            => ExecAsync(r => r.IncrementValueByAsync(key, (int)amount, cancellationToken));

        ValueTask<long> ICacheClientAsync.DecrementAsync(string key, uint amount, CancellationToken cancellationToken)
            => ExecAsync(r => r.DecrementValueByAsync(key, (int)amount, cancellationToken));


        ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, CancellationToken cancellationToken)
            => ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), exists: false, cancellationToken: cancellationToken));

        ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, CancellationToken cancellationToken)
            => ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), exists: true, cancellationToken: cancellationToken));

        ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            AssertNotInTransaction();

            return ExecAsync(async r =>
            {
                if (await r.AddAsync(key, value, cancellationToken).ConfigureAwait(false))
                {
                    await r.ExpireEntryAtAsync(key, ConvertToServerDate(expiresAt), cancellationToken).ConfigureAwait(false);
                    return true;
                }
                return false;
            });
        }

        ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, DateTime expiresAt, CancellationToken cancellationToken)
        {
            AssertNotInTransaction();

            return ExecAsync(async r =>
            {
                if (await r.ReplaceAsync(key, value, cancellationToken).ConfigureAwait(false))
                {
                    await r.ExpireEntryAtAsync(key, ConvertToServerDate(expiresAt), cancellationToken).ConfigureAwait(false);
                    return true;
                }
                return false;
            });
        }

        ValueTask<bool> ICacheClientAsync.AddAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
            => ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), exists: false, cancellationToken: cancellationToken));

        ValueTask<bool> ICacheClientAsync.ReplaceAsync<T>(string key, T value, TimeSpan expiresIn, CancellationToken cancellationToken)
            => ExecAsync(r => ((IRedisNativeClientAsync)r).SetAsync(key, ToBytes(value), exists: true, cancellationToken: cancellationToken));

        ValueTask<long> IRedisClientAsync.DbSizeAsync(CancellationToken cancellationToken)
            => NativeAsync.DbSizeAsync(cancellationToken);

        ValueTask<Dictionary<string, string>> IRedisClientAsync.InfoAsync(CancellationToken cancellationToken)
            => NativeAsync.InfoAsync(cancellationToken);

        ValueTask<DateTime> IRedisClientAsync.LastSaveAsync(CancellationToken cancellationToken)
            => NativeAsync.LastSaveAsync(cancellationToken);

        async ValueTask<T> IEntityStoreAsync.GetByIdAsync<T>(object id, CancellationToken cancellationToken)
        {
            var key = UrnKey<T>(id);
            var valueString = await AsAsync().GetValueAsync(key, cancellationToken).ConfigureAwait(false);
            var value = JsonSerializer.DeserializeFromString<T>(valueString);
            return value;
        }

        async ValueTask<IList<T>> IEntityStoreAsync.GetByIdsAsync<T>(ICollection ids, CancellationToken cancellationToken)
        {
            if (ids == null || ids.Count == 0)
                return new List<T>();

            var urnKeys = ids.Cast<object>().Map(UrnKey<T>);
            return await AsAsync().GetValuesAsync<T>(urnKeys, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<T> IEntityStoreAsync.StoreAsync<T>(T entity, CancellationToken cancellationToken)
        {
            var urnKey = UrnKey(entity);
            var valueString = JsonSerializer.SerializeToString(entity);

            await AsAsync().SetValueAsync(urnKey, valueString, cancellationToken).ConfigureAwait(false);
            await RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);

            return entity;
        }

        ValueTask IEntityStoreAsync.StoreAllAsync<TEntity>(IEnumerable<TEntity> entities, CancellationToken cancellationToken)
            => StoreAllAsyncImpl(entities, cancellationToken);

        internal async ValueTask StoreAllAsyncImpl<TEntity>(IEnumerable<TEntity> entities, CancellationToken cancellationToken)
        {
            if (PrepareStoreAll(entities, out var keys, out var values, out var entitiesList))
            {
                await NativeAsync.MSetAsync(keys, values, cancellationToken).ConfigureAwait(false);
                await RegisterTypeIdsAsync(entitiesList, cancellationToken).ConfigureAwait(false);
            }
        }

        internal ValueTask RegisterTypeIdsAsync<T>(IEnumerable<T> values, CancellationToken cancellationToken)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            var ids = values.Map(x => x.GetId().ToString());

            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                ids.ForEach(x => registeredTypeIdsWithinPipeline.Add(x));
                return default;
            }
            else
            {
                return AsAsync().AddRangeToSetAsync(typeIdsSetKey, ids, cancellationToken);
            }
        }

        internal async ValueTask RemoveTypeIdsAsync<T>(T[] values, CancellationToken cancellationToken)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                values.Each(x => registeredTypeIdsWithinPipeline.Remove(x.GetId().ToString()));
            }
            else
            {
                foreach (var x in values)
                {
                    await AsAsync().RemoveItemFromSetAsync(typeIdsSetKey, x.GetId().ToString(), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        internal async ValueTask RemoveTypeIdsAsync<T>(string[] ids, CancellationToken cancellationToken)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                ids.Each(x => registeredTypeIdsWithinPipeline.Remove(x));
            }
            else
            {
                foreach (var x in ids)
                {
                    await AsAsync().RemoveItemFromSetAsync(typeIdsSetKey, x, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        async ValueTask IEntityStoreAsync.DeleteAsync<T>(T entity, CancellationToken cancellationToken)
        {
            var urnKey = UrnKey(entity);
            await AsAsync().RemoveAsync(urnKey, cancellationToken).ConfigureAwait(false);
            await this.RemoveTypeIdsAsync(new[] { entity }, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask IEntityStoreAsync.DeleteByIdAsync<T>(object id, CancellationToken cancellationToken)
        {
            var urnKey = UrnKey<T>(id);
            await AsAsync().RemoveAsync(urnKey, cancellationToken).ConfigureAwait(false);
            await this.RemoveTypeIdsAsync<T>(new[] { id.ToString() }, cancellationToken).ConfigureAwait(false);
        }

        async ValueTask IEntityStoreAsync.DeleteByIdsAsync<T>(ICollection ids, CancellationToken cancellationToken)
        {
            if (ids == null || ids.Count == 0) return;

            var idsList = ids.Cast<object>();
            var urnKeys = idsList.Map(UrnKey<T>);
            await AsAsync().RemoveEntryAsync(urnKeys.ToArray(), cancellationToken).ConfigureAwait(false);
            await this.RemoveTypeIdsAsync<T>(idsList.Map(x => x.ToString()).ToArray(), cancellationToken).ConfigureAwait(false);
        }

        async ValueTask IEntityStoreAsync.DeleteAllAsync<T>(CancellationToken cancellationToken)
        {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            var ids = await AsAsync().GetAllItemsFromSetAsync(typeIdsSetKey, cancellationToken).ConfigureAwait(false);
            if (ids.Count > 0)
            {
                var urnKeys = ids.ToList().ConvertAll(UrnKey<T>);
                await AsAsync().RemoveEntryAsync(urnKeys.ToArray()).ConfigureAwait(false);
                await AsAsync().RemoveAsync(typeIdsSetKey).ConfigureAwait(false);
            }
        }

        ValueTask<List<string>> IRedisClientAsync.SearchSortedSetAsync(string setId, string start, string end, int? skip, int? take, CancellationToken cancellationToken)
        {
            start = GetSearchStart(start);
            end = GetSearchEnd(end);

            return NativeAsync.ZRangeByLexAsync(setId, start, end, skip, take, cancellationToken).ToStringListAsync();
        }

        ValueTask<long> IRedisClientAsync.SearchSortedSetCountAsync(string setId, string start, string end, CancellationToken cancellationToken)
            => NativeAsync.ZLexCountAsync(setId, GetSearchStart(start), GetSearchEnd(end), cancellationToken);

        ValueTask<long> IRedisClientAsync.RemoveRangeFromSortedSetBySearchAsync(string setId, string start, string end, CancellationToken cancellationToken)
            => NativeAsync.ZRemRangeByLexAsync(setId, GetSearchStart(start), GetSearchEnd(end), cancellationToken);

        ValueTask<string> IRedisClientAsync.TypeAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.TypeAsync(key, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetStringCountAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.StrLenAsync(key, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetSetCountAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.SCardAsync(setId, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetListCountAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.LLenAsync(listId, cancellationToken);

        ValueTask<long> IRedisClientAsync.GetHashCountAsync(string hashId, CancellationToken cancellationToken)
            => NativeAsync.HLenAsync(hashId, cancellationToken);

        async ValueTask<T> IRedisClientAsync.ExecCachedLuaAsync<T>(string scriptBody, Func<string, ValueTask<T>> scriptSha1, CancellationToken cancellationToken)
        {
            if (!CachedLuaSha1Map.TryGetValue(scriptBody, out var sha1))
                CachedLuaSha1Map[scriptBody] = sha1 = await AsAsync().LoadLuaScriptAsync(scriptBody, cancellationToken).ConfigureAwait(false);

            try
            {
                return await scriptSha1(sha1).ConfigureAwait(false);
            }
            catch (RedisResponseException ex)
            {
                if (!ex.Message.StartsWith("NOSCRIPT"))
                    throw;

                CachedLuaSha1Map[scriptBody] = sha1 = await AsAsync().LoadLuaScriptAsync(scriptBody, cancellationToken).ConfigureAwait(false);
                return await scriptSha1(sha1).ConfigureAwait(false);
            }
        }

        ValueTask<RedisText> IRedisClientAsync.ExecLuaAsync(string luaBody, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalCommandAsync(luaBody, keys?.Length ?? 0, MergeAndConvertToBytes(keys, args), cancellationToken).Await(data => data.ToRedisText());

        ValueTask<RedisText> IRedisClientAsync.ExecLuaShaAsync(string sha1, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaCommandAsync(sha1, keys?.Length ?? 0, MergeAndConvertToBytes(keys, args), cancellationToken).Await(data => data.ToRedisText());

        ValueTask<string> IRedisClientAsync.ExecLuaAsStringAsync(string luaBody, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalStrAsync(luaBody, keys?.Length ?? 0, MergeAndConvertToBytes(keys, args), cancellationToken);

        ValueTask<string> IRedisClientAsync.ExecLuaShaAsStringAsync(string sha1, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaStrAsync(sha1, keys?.Length ?? 0, MergeAndConvertToBytes(keys, args), cancellationToken);

        ValueTask<string> IRedisClientAsync.LoadLuaScriptAsync(string body, CancellationToken cancellationToken)
            => NativeAsync.ScriptLoadAsync(body, cancellationToken).FromUtf8BytesAsync();

        ValueTask IRedisClientAsync.WriteAllAsync<TEntity>(IEnumerable<TEntity> entities, CancellationToken cancellationToken)
            => PrepareWriteAll(entities, out var keys, out var values) ? NativeAsync.MSetAsync(keys, values, cancellationToken) : default;

        async ValueTask<HashSet<string>> IRedisClientAsync.GetAllItemsFromSetAsync(string setId, CancellationToken cancellationToken)
        {
            var multiDataList = await NativeAsync.SMembersAsync(setId, cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        async ValueTask IRedisClientAsync.AddRangeToSetAsync(string setId, List<string> items, CancellationToken cancellationToken)
        {
            if (await AddRangeToSetNeedsSendAsync(setId, items).ConfigureAwait(false))
            {
                var uSetId = setId.ToUtf8Bytes();
                var pipeline = CreatePipelineCommand();
                foreach (var item in items)
                {
                    pipeline.WriteCommand(Commands.SAdd, uSetId, item.ToUtf8Bytes());
                }
                await pipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

                //the number of items after
                _ = await pipeline.ReadAllAsIntsAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        async ValueTask<bool> AddRangeToSetNeedsSendAsync(string setId, List<string> items)
        {
            if (setId.IsNullOrEmpty())
                throw new ArgumentNullException("setId");
            if (items == null)
                throw new ArgumentNullException("items");
            if (items.Count == 0)
                return false;

            if (this.Transaction is object || this.PipelineAsync is object)
            {
                var queueable = this.Transaction as IRedisQueueableOperationAsync
                    ?? this.Pipeline as IRedisQueueableOperationAsync;

                if (queueable == null)
                    throw new NotSupportedException("Cannot AddRangeToSetAsync() when Transaction is: " + this.Transaction.GetType().Name);

                //Complete the first QueuedCommand()
                await AsAsync().AddItemToSetAsync(setId, items[0]).ConfigureAwait(false);

                //Add subsequent queued commands
                for (var i = 1; i < items.Count; i++)
                {
                    var item = items[i];
                    queueable.QueueCommand(c => c.AddItemToSetAsync(setId, item));
                }
                return false;
            }
            else
            {
                return true;
            }
        }

        ValueTask IRedisClientAsync.RemoveItemFromSetAsync(string setId, string item, CancellationToken cancellationToken)
            => NativeAsync.SRemAsync(setId, item.ToUtf8Bytes(), cancellationToken).Await();

        ValueTask<long> IRedisClientAsync.IncrementValueByAsync(string key, int count, CancellationToken cancellationToken)
            => NativeAsync.IncrByAsync(key, count, cancellationToken);

        ValueTask<long> IRedisClientAsync.IncrementValueByAsync(string key, long count, CancellationToken cancellationToken)
            => NativeAsync.IncrByAsync(key, count, cancellationToken);

        ValueTask<double> IRedisClientAsync.IncrementValueByAsync(string key, double count, CancellationToken cancellationToken)
            => NativeAsync.IncrByFloatAsync(key, count, cancellationToken);
        ValueTask<long> IRedisClientAsync.IncrementValueAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.IncrAsync(key, cancellationToken);

        ValueTask<long> IRedisClientAsync.DecrementValueAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.DecrAsync(key, cancellationToken);

        ValueTask<long> IRedisClientAsync.DecrementValueByAsync(string key, int count, CancellationToken cancellationToken)
            => NativeAsync.DecrByAsync(key, count, cancellationToken);

        async ValueTask<RedisServerRole> IRedisClientAsync.GetServerRoleAsync(CancellationToken cancellationToken)
        {
            if (AssertServerVersionNumber() >= 2812)
            {
                var text = await NativeAsync.RoleAsync(cancellationToken).ConfigureAwait(false);
                var roleName = text.Children[0].Text;
                return ToServerRole(roleName);
            }

            var info = await AsAsync().InfoAsync(cancellationToken).ConfigureAwait(false);
            info.TryGetValue("role", out var role);
            return ToServerRole(role);
        }

        ValueTask<RedisText> IRedisClientAsync.GetServerRoleInfoAsync(CancellationToken cancellationToken)
            => NativeAsync.RoleAsync(cancellationToken);

        async ValueTask<string> IRedisClientAsync.GetConfigAsync(string configItem, CancellationToken cancellationToken)
        {
            var byteArray = await NativeAsync.ConfigGetAsync(configItem, cancellationToken).ConfigureAwait(false);
            return GetConfigParse(byteArray);
        }

        ValueTask IRedisClientAsync.SetConfigAsync(string configItem, string value, CancellationToken cancellationToken)
            => NativeAsync.ConfigSetAsync(configItem, value.ToUtf8Bytes(), cancellationToken);

        ValueTask IRedisClientAsync.SaveConfigAsync(CancellationToken cancellationToken)
            => NativeAsync.ConfigRewriteAsync(cancellationToken);

        ValueTask IRedisClientAsync.ResetInfoStatsAsync(CancellationToken cancellationToken)
            => NativeAsync.ConfigResetStatAsync(cancellationToken);

        ValueTask<string> IRedisClientAsync.GetClientAsync(CancellationToken cancellationToken)
            => NativeAsync.ClientGetNameAsync(cancellationToken);

        ValueTask IRedisClientAsync.SetClientAsync(string name, CancellationToken cancellationToken)
            => NativeAsync.ClientSetNameAsync(name, cancellationToken);

        ValueTask IRedisClientAsync.KillClientAsync(string address, CancellationToken cancellationToken)
            => NativeAsync.ClientKillAsync(address, cancellationToken);

        ValueTask<long> IRedisClientAsync.KillClientsAsync(string fromAddress, string withId, RedisClientType? ofType, bool? skipMe, CancellationToken cancellationToken)
        {
            var typeString = ofType?.ToString().ToLower();
            var skipMeString = skipMe.HasValue ? (skipMe.Value ? "yes" : "no") : null;
            return NativeAsync.ClientKillAsync(addr: fromAddress, id: withId, type: typeString, skipMe: skipMeString, cancellationToken);
        }

        async ValueTask<List<Dictionary<string, string>>> IRedisClientAsync.GetClientsInfoAsync(CancellationToken cancellationToken)
            => GetClientsInfoParse(await NativeAsync.ClientListAsync(cancellationToken).ConfigureAwait(false));

        ValueTask IRedisClientAsync.PauseAllClientsAsync(TimeSpan duration, CancellationToken cancellationToken)
            => NativeAsync.ClientPauseAsync((int)duration.TotalMilliseconds, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetAllKeysAsync(CancellationToken cancellationToken)
            => AsAsync().SearchKeysAsync("*", cancellationToken);

        ValueTask<string> IRedisClientAsync.GetAndSetValueAsync(string key, string value, CancellationToken cancellationToken)
            => NativeAsync.GetSetAsync(key, value.ToUtf8Bytes(), cancellationToken).FromUtf8BytesAsync();

        async ValueTask<T> IRedisClientAsync.GetFromHashAsync<T>(object id, CancellationToken cancellationToken)
        {
            var key = UrnKey<T>(id);
            return (await AsAsync().GetAllEntriesFromHashAsync(key, cancellationToken).ConfigureAwait(false)).ToJson().FromJson<T>();
        }

        async ValueTask IRedisClientAsync.StoreAsHashAsync<T>(T entity, CancellationToken cancellationToken)
        {
            var key = UrnKey(entity);
            var hash = ConvertToHashFn(entity);
            await AsAsync().SetRangeInHashAsync(key, hash, cancellationToken).ConfigureAwait(false);
            await RegisterTypeIdAsync(entity, cancellationToken).ConfigureAwait(false);
        }

        ValueTask<List<string>> IRedisClientAsync.GetSortedEntryValuesAsync(string setId, int startingFrom, int endingAt, CancellationToken cancellationToken)
        {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, };
            return NativeAsync.SortAsync(setId, sortOptions, cancellationToken).ToStringListAsync();
        }

        async IAsyncEnumerable<string> IRedisClientAsync.ScanAllSetItemsAsync(string setId, string pattern, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var ret = new ScanResult();
            while (true)
            {
                ret = await (pattern != null // note ConfigureAwait is handled below
                    ? NativeAsync.SScanAsync(setId, ret.Cursor, pageSize, match: pattern, cancellationToken: cancellationToken)
                    : NativeAsync.SScanAsync(setId, ret.Cursor, pageSize, cancellationToken: cancellationToken)
                    ).ConfigureAwait(false);

                foreach (var key in ret.Results)
                {
                    yield return key.FromUtf8Bytes();
                }

                if (ret.Cursor == 0) break;
            }
        }

        async IAsyncEnumerable<KeyValuePair<string, double>> IRedisClientAsync.ScanAllSortedSetItemsAsync(string setId, string pattern, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var ret = new ScanResult();
            while (true)
            {
                ret = await (pattern != null // note ConfigureAwait is handled below
                    ? NativeAsync.ZScanAsync(setId, ret.Cursor, pageSize, match: pattern, cancellationToken: cancellationToken)
                    : NativeAsync.ZScanAsync(setId, ret.Cursor, pageSize, cancellationToken: cancellationToken)
                    ).ConfigureAwait(false);

                foreach (var entry in ret.AsItemsWithScores())
                {
                    yield return entry;
                }

                if (ret.Cursor == 0) break;
            }
        }

        async IAsyncEnumerable<KeyValuePair<string, string>> IRedisClientAsync.ScanAllHashEntriesAsync(string hashId, string pattern, int pageSize, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var ret = new ScanResult();
            while (true)
            {
                ret = await (pattern != null // note ConfigureAwait is handled below
                    ? NativeAsync.HScanAsync(hashId, ret.Cursor, pageSize, match: pattern, cancellationToken: cancellationToken)
                    : NativeAsync.HScanAsync(hashId, ret.Cursor, pageSize, cancellationToken: cancellationToken)
                    ).ConfigureAwait(false);

                foreach (var entry in ret.AsKeyValues())
                {
                    yield return entry;
                }

                if (ret.Cursor == 0) break;
            }
        }

        ValueTask<bool> IRedisClientAsync.AddToHyperLogAsync(string key, string[] elements, CancellationToken cancellationToken)
            => NativeAsync.PfAddAsync(key, elements.Map(x => x.ToUtf8Bytes()).ToArray(), cancellationToken);

        ValueTask<long> IRedisClientAsync.CountHyperLogAsync(string key, CancellationToken cancellationToken)
            => NativeAsync.PfCountAsync(key, cancellationToken);

        ValueTask IRedisClientAsync.MergeHyperLogsAsync(string toKey, string[] fromKeys, CancellationToken cancellationToken)
            => NativeAsync.PfMergeAsync(toKey, fromKeys, cancellationToken);

        ValueTask<long> IRedisClientAsync.AddGeoMemberAsync(string key, double longitude, double latitude, string member, CancellationToken cancellationToken)
            => NativeAsync.GeoAddAsync(key, longitude, latitude, member, cancellationToken);

        ValueTask<long> IRedisClientAsync.AddGeoMembersAsync(string key, RedisGeo[] geoPoints, CancellationToken cancellationToken)
            => NativeAsync.GeoAddAsync(key, geoPoints, cancellationToken);

        ValueTask<double> IRedisClientAsync.CalculateDistanceBetweenGeoMembersAsync(string key, string fromMember, string toMember, string unit, CancellationToken cancellationToken)
            => NativeAsync.GeoDistAsync(key, fromMember, toMember, unit, cancellationToken);

        ValueTask<string[]> IRedisClientAsync.GetGeohashesAsync(string key, string[] members, CancellationToken cancellationToken)
            => NativeAsync.GeoHashAsync(key, members, cancellationToken);

        ValueTask<List<RedisGeo>> IRedisClientAsync.GetGeoCoordinatesAsync(string key, string[] members, CancellationToken cancellationToken)
            => NativeAsync.GeoPosAsync(key, members, cancellationToken);

        async ValueTask<string[]> IRedisClientAsync.FindGeoMembersInRadiusAsync(string key, double longitude, double latitude, double radius, string unit, CancellationToken cancellationToken)
        {
            var results = await NativeAsync.GeoRadiusAsync(key, longitude, latitude, radius, unit, cancellationToken: cancellationToken).ConfigureAwait(false);
            return ParseFindGeoMembersResult(results);
        }

        ValueTask<List<RedisGeoResult>> IRedisClientAsync.FindGeoResultsInRadiusAsync(string key, double longitude, double latitude, double radius, string unit, int? count, bool? sortByNearest, CancellationToken cancellationToken)
            => NativeAsync.GeoRadiusAsync(key, longitude, latitude, radius, unit, withCoords: true, withDist: true, withHash: true, count: count, asc: sortByNearest, cancellationToken: cancellationToken);

        async ValueTask<string[]> IRedisClientAsync.FindGeoMembersInRadiusAsync(string key, string member, double radius, string unit, CancellationToken cancellationToken)
        {
            var results = await NativeAsync.GeoRadiusByMemberAsync(key, member, radius, unit, cancellationToken: cancellationToken).ConfigureAwait(false);
            return ParseFindGeoMembersResult(results);
        }

        ValueTask<List<RedisGeoResult>> IRedisClientAsync.FindGeoResultsInRadiusAsync(string key, string member, double radius, string unit, int? count, bool? sortByNearest, CancellationToken cancellationToken)
            => NativeAsync.GeoRadiusByMemberAsync(key, member, radius, unit, withCoords: true, withDist: true, withHash: true, count: count, asc: sortByNearest, cancellationToken: cancellationToken);

        ValueTask<IRedisSubscriptionAsync> IRedisClientAsync.CreateSubscriptionAsync(CancellationToken cancellationToken)
            => new RedisSubscription(this).AsValueTask<IRedisSubscriptionAsync>();

        ValueTask<long> IRedisClientAsync.PublishMessageAsync(string toChannel, string message, CancellationToken cancellationToken)
            => NativeAsync.PublishAsync(toChannel, message.ToUtf8Bytes(), cancellationToken);

        ValueTask IRedisClientAsync.MoveBetweenSetsAsync(string fromSetId, string toSetId, string item, CancellationToken cancellationToken)
            => NativeAsync.SMoveAsync(fromSetId, toSetId, item.ToUtf8Bytes(), cancellationToken);

        ValueTask<bool> IRedisClientAsync.SetContainsItemAsync(string setId, string item, CancellationToken cancellationToken)
            => NativeAsync.SIsMemberAsync(setId, item.ToUtf8Bytes(), cancellationToken).IsSuccessAsync();

        async ValueTask<HashSet<string>> IRedisClientAsync.GetIntersectFromSetsAsync(string[] setIds, CancellationToken cancellationToken)
        {
            if (setIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = await NativeAsync.SInterAsync(setIds, cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisClientAsync.StoreIntersectFromSetsAsync(string intoSetId, string[] setIds, CancellationToken cancellationToken)
        {
            if (setIds.Length == 0) return default;

            return NativeAsync.SInterStoreAsync(intoSetId, setIds, cancellationToken);
        }

        async ValueTask<HashSet<string>> IRedisClientAsync.GetUnionFromSetsAsync(string[] setIds, CancellationToken cancellationToken)
        {
            if (setIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = await NativeAsync.SUnionAsync(setIds, cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisClientAsync.StoreUnionFromSetsAsync(string intoSetId, string[] setIds, CancellationToken cancellationToken)
        {
            if (setIds.Length == 0) return default;

            return NativeAsync.SUnionStoreAsync(intoSetId, setIds, cancellationToken);
        }

        async ValueTask<HashSet<string>> IRedisClientAsync.GetDifferencesFromSetAsync(string fromSetId, string[] withSetIds, CancellationToken cancellationToken)
        {
            if (withSetIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = await NativeAsync.SDiffAsync(fromSetId, withSetIds, cancellationToken).ConfigureAwait(false);
            return CreateHashSet(multiDataList);
        }

        ValueTask IRedisClientAsync.StoreDifferencesFromSetAsync(string intoSetId, string fromSetId, string[] withSetIds, CancellationToken cancellationToken)
        {
            if (withSetIds.Length == 0) return default;

            return NativeAsync.SDiffStoreAsync(intoSetId, fromSetId, withSetIds, cancellationToken);
        }

        ValueTask<string> IRedisClientAsync.GetRandomItemFromSetAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.SRandMemberAsync(setId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<List<string>> IRedisClientAsync.GetAllItemsFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.LRangeAsync(listId, FirstElement, LastElement, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromListAsync(string listId, int startingFrom, int endingAt, CancellationToken cancellationToken)
            => NativeAsync.LRangeAsync(listId, startingFrom, endingAt, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedListAsync(string listId, int startingFrom, int endingAt, CancellationToken cancellationToken)
        {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, SortAlpha = true };
            return AsAsync().GetSortedItemsFromListAsync(listId, sortOptions, cancellationToken);
        }

        ValueTask<List<string>> IRedisClientAsync.GetSortedItemsFromListAsync(string listId, SortOptions sortOptions, CancellationToken cancellationToken)
            => NativeAsync.SortAsync(listId, sortOptions, cancellationToken).ToStringListAsync();

        async ValueTask IRedisClientAsync.AddRangeToListAsync(string listId, List<string> values, CancellationToken cancellationToken)
        {
            var pipeline = AddRangeToListPrepareNonFlushed(listId, values);
            await pipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

            //the number of items after
            _ = await pipeline.ReadAllAsIntsAsync(cancellationToken).ConfigureAwait(false);
        }

        ValueTask IRedisClientAsync.PrependItemToListAsync(string listId, string value, CancellationToken cancellationToken)
            => NativeAsync.LPushAsync(listId, value.ToUtf8Bytes(), cancellationToken).Await();

        async ValueTask IRedisClientAsync.PrependRangeToListAsync(string listId, List<string> values, CancellationToken cancellationToken)
        {
            var pipeline = PrependRangeToListPrepareNonFlushed(listId, values);
            await pipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

            //the number of items after
            _ = await pipeline.ReadAllAsIntsAsync(cancellationToken).ConfigureAwait(false);
        }

        ValueTask IRedisClientAsync.RemoveAllFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.LTrimAsync(listId, LastElement, FirstElement, cancellationToken);

        ValueTask<string> IRedisClientAsync.RemoveStartFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.LPopAsync(listId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<string> IRedisClientAsync.BlockingRemoveStartFromListAsync(string listId, TimeSpan? timeOut, CancellationToken cancellationToken)
            => NativeAsync.BLPopValueAsync(listId, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).FromUtf8BytesAsync();

        async ValueTask<ItemRef> IRedisClientAsync.BlockingRemoveStartFromListsAsync(string[] listIds, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var value = await NativeAsync.BLPopValueAsync(listIds, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            if (value == null)
                return null;
            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        ValueTask<string> IRedisClientAsync.RemoveEndFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.RPopAsync(listId, cancellationToken).FromUtf8BytesAsync();

        ValueTask IRedisClientAsync.TrimListAsync(string listId, int keepStartingFrom, int keepEndingAt, CancellationToken cancellationToken)
            => NativeAsync.LTrimAsync(listId, keepStartingFrom, keepEndingAt, cancellationToken);

        ValueTask<long> IRedisClientAsync.RemoveItemFromListAsync(string listId, string value, CancellationToken cancellationToken)
            => NativeAsync.LRemAsync(listId, 0, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<long> IRedisClientAsync.RemoveItemFromListAsync(string listId, string value, int noOfMatches, CancellationToken cancellationToken)
            => NativeAsync.LRemAsync(listId, 0, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<string> IRedisClientAsync.GetItemFromListAsync(string listId, int listIndex, CancellationToken cancellationToken)
            => NativeAsync.LIndexAsync(listId, listIndex, cancellationToken).FromUtf8BytesAsync();

        ValueTask IRedisClientAsync.SetItemInListAsync(string listId, int listIndex, string value, CancellationToken cancellationToken)
            => NativeAsync.LSetAsync(listId, listIndex, value.ToUtf8Bytes(), cancellationToken);

        ValueTask IRedisClientAsync.EnqueueItemOnListAsync(string listId, string value, CancellationToken cancellationToken)
            => NativeAsync.LPushAsync(listId, value.ToUtf8Bytes(), cancellationToken).Await();

        ValueTask<string> IRedisClientAsync.DequeueItemFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.RPopAsync(listId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<string> IRedisClientAsync.BlockingDequeueItemFromListAsync(string listId, TimeSpan? timeOut, CancellationToken cancellationToken)
            => NativeAsync.BRPopValueAsync(listId, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).FromUtf8BytesAsync();

        async ValueTask<ItemRef> IRedisClientAsync.BlockingDequeueItemFromListsAsync(string[] listIds, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var value = await NativeAsync.BRPopValueAsync(listIds, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            if (value == null)
                return null;
            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        ValueTask IRedisClientAsync.PushItemToListAsync(string listId, string value, CancellationToken cancellationToken)
            => NativeAsync.RPushAsync(listId, value.ToUtf8Bytes(), cancellationToken).Await();

        ValueTask<string> IRedisClientAsync.PopItemFromListAsync(string listId, CancellationToken cancellationToken)
            => NativeAsync.RPopAsync(listId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<string> IRedisClientAsync.BlockingPopItemFromListAsync(string listId, TimeSpan? timeOut, CancellationToken cancellationToken)
            => NativeAsync.BRPopValueAsync(listId, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).FromUtf8BytesAsync();

        async ValueTask<ItemRef> IRedisClientAsync.BlockingPopItemFromListsAsync(string[] listIds, TimeSpan? timeOut, CancellationToken cancellationToken)
        {
            var value = await NativeAsync.BRPopValueAsync(listIds, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).ConfigureAwait(false);
            if (value == null)
                return null;
            return new ItemRef { Id = value[0].FromUtf8Bytes(), Item = value[1].FromUtf8Bytes() };
        }

        ValueTask<string> IRedisClientAsync.PopAndPushItemBetweenListsAsync(string fromListId, string toListId, CancellationToken cancellationToken)
            => NativeAsync.RPopLPushAsync(fromListId, toListId, cancellationToken).FromUtf8BytesAsync();

        ValueTask<string> IRedisClientAsync.BlockingPopAndPushItemBetweenListsAsync(string fromListId, string toListId, TimeSpan? timeOut, CancellationToken cancellationToken)
            => NativeAsync.BRPopLPushAsync(fromListId, toListId, (int)timeOut.GetValueOrDefault().TotalSeconds, cancellationToken).FromUtf8BytesAsync();

        async ValueTask<bool> IRedisClientAsync.AddRangeToSortedSetAsync(string setId, List<string> values, double score, CancellationToken cancellationToken)
        {
            var pipeline = AddRangeToSortedSetPrepareNonFlushed(setId, values, score.ToFastUtf8Bytes());
            await pipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

            return await pipeline.ReadAllAsIntsHaveSuccessAsync(cancellationToken).ConfigureAwait(false);
        }

        async ValueTask<bool> IRedisClientAsync.AddRangeToSortedSetAsync(string setId, List<string> values, long score, CancellationToken cancellationToken)
        {
            var pipeline = AddRangeToSortedSetPrepareNonFlushed(setId, values, score.ToUtf8Bytes());
            await pipeline.FlushAsync(cancellationToken).ConfigureAwait(false);

            return await pipeline.ReadAllAsIntsHaveSuccessAsync(cancellationToken).ConfigureAwait(false);
        }

        ValueTask<bool> IRedisClientAsync.RemoveItemFromSortedSetAsync(string setId, string value, CancellationToken cancellationToken)
            => NativeAsync.ZRemAsync(setId, value.ToUtf8Bytes(), cancellationToken).IsSuccessAsync();

        ValueTask<long> IRedisClientAsync.RemoveItemsFromSortedSetAsync(string setId, List<string> values, CancellationToken cancellationToken)
            => NativeAsync.ZRemAsync(setId, values.Map(x => x.ToUtf8Bytes()).ToArray(), cancellationToken);

        async ValueTask<string> IRedisClientAsync.PopItemWithLowestScoreFromSortedSetAsync(string setId, CancellationToken cancellationToken)
        {
            //TODO: this should be atomic
            var topScoreItemBytes = await NativeAsync.ZRangeAsync(setId, FirstElement, 1, cancellationToken).ConfigureAwait(false);
            if (topScoreItemBytes.Length == 0) return null;

            await NativeAsync.ZRemAsync(setId, topScoreItemBytes[0], cancellationToken).ConfigureAwait(false);
            return topScoreItemBytes[0].FromUtf8Bytes();
        }

       async ValueTask<string> IRedisClientAsync.PopItemWithHighestScoreFromSortedSetAsync(string setId, CancellationToken cancellationToken)
        {
            //TODO: this should be atomic
            var topScoreItemBytes = await NativeAsync.ZRevRangeAsync(setId, FirstElement, 1, cancellationToken).ConfigureAwait(false);
            if (topScoreItemBytes.Length == 0) return null;

            await NativeAsync.ZRemAsync(setId, topScoreItemBytes[0], cancellationToken).ConfigureAwait(false);
            return topScoreItemBytes[0].FromUtf8Bytes();
        }

        ValueTask<bool> IRedisClientAsync.SortedSetContainsItemAsync(string setId, string value, CancellationToken cancellationToken)
            => NativeAsync.ZRankAsync(setId, value.ToUtf8Bytes(),cancellationToken).Await(val => val != -1);

        ValueTask<double> IRedisClientAsync.IncrementItemInSortedSetAsync(string setId, string value, double incrementBy, CancellationToken cancellationToken)
            => NativeAsync.ZIncrByAsync(setId, incrementBy, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<double> IRedisClientAsync.IncrementItemInSortedSetAsync(string setId, string value, long incrementBy, CancellationToken cancellationToken)
            => NativeAsync.ZIncrByAsync(setId, incrementBy, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<long> IRedisClientAsync.GetItemIndexInSortedSetAsync(string setId, string value, CancellationToken cancellationToken)
            => NativeAsync.ZRankAsync(setId, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<long> IRedisClientAsync.GetItemIndexInSortedSetDescAsync(string setId, string value, CancellationToken cancellationToken)
            => NativeAsync.ZRevRankAsync(setId, value.ToUtf8Bytes(), cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetAllItemsFromSortedSetAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.ZRangeAsync(setId, FirstElement, LastElement, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetAllItemsFromSortedSetDescAsync(string setId, CancellationToken cancellationToken)
            => NativeAsync.ZRevRangeAsync(setId, FirstElement, LastElement, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetAsync(string setId, int fromRank, int toRank, CancellationToken cancellationToken)
            => NativeAsync.ZRangeAsync(setId, fromRank, toRank, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetDescAsync(string setId, int fromRank, int toRank, CancellationToken cancellationToken)
            => NativeAsync.ZRevRangeAsync(setId, fromRank, toRank, cancellationToken).ToStringListAsync();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ValueTask<IDictionary<string, double>> CreateSortedScoreMapAsync(ValueTask<byte[][]> pending)
        {
            return pending.IsCompletedSuccessfully ? CreateSortedScoreMap(pending.Result).AsValueTask() : Awaited(pending);
            static async ValueTask<IDictionary<string, double>> Awaited(ValueTask<byte[][]> pending)
                => CreateSortedScoreMap(await pending.ConfigureAwait(false));
        }

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetAllWithScoresFromSortedSetAsync(string setId, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRangeWithScoresAsync(setId, FirstElement, LastElement, cancellationToken));

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetAsync(string setId, int fromRank, int toRank, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRangeWithScoresAsync(setId, fromRank, toRank, cancellationToken));

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetDescAsync(string setId, int fromRank, int toRank, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRevRangeWithScoresAsync(setId, fromRank, toRank, cancellationToken));

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByLowestScoreAsync(setId, fromStringScore, toStringScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
        {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return AsAsync().GetRangeFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken);
        }

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => NativeAsync.ZRangeByScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByLowestScoreAsync(string setId, long fromScore, long toScore, int? skip, int? take, CancellationToken cancellationToken)
            => NativeAsync.ZRangeByScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken).ToStringListAsync();

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByLowestScoreAsync(setId, fromStringScore, toStringScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
        {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return AsAsync().GetRangeWithScoresFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken);
        }

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByLowestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRangeByScoreWithScoresAsync(setId, fromScore, toScore, skip, take, cancellationToken));

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByLowestScoreAsync(string setId, long fromScore, long toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRangeByScoreWithScoresAsync(setId, fromScore, toScore, skip, take, cancellationToken));

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByHighestScoreAsync(setId, fromStringScore, toStringScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
        {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return AsAsync().GetRangeFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken);
        }

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => NativeAsync.ZRevRangeByScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetRangeFromSortedSetByHighestScoreAsync(string setId, long fromScore, long toScore, int? skip, int? take, CancellationToken cancellationToken)
            => NativeAsync.ZRevRangeByScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken).ToStringListAsync();

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, string fromStringScore, string toStringScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByHighestScoreAsync(setId, fromStringScore, toStringScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, string fromStringScore, string toStringScore, int? skip, int? take, CancellationToken cancellationToken)
        {
            var fromScore = GetLexicalScore(fromStringScore);
            var toScore = GetLexicalScore(toStringScore);
            return AsAsync().GetRangeWithScoresFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, skip, take, cancellationToken);
        }

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => AsAsync().GetRangeWithScoresFromSortedSetByHighestScoreAsync(setId, fromScore, toScore, null, null, cancellationToken);

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, double fromScore, double toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRevRangeByScoreWithScoresAsync(setId, fromScore, toScore, skip, take, cancellationToken));

        ValueTask<IDictionary<string, double>> IRedisClientAsync.GetRangeWithScoresFromSortedSetByHighestScoreAsync(string setId, long fromScore, long toScore, int? skip, int? take, CancellationToken cancellationToken)
            => CreateSortedScoreMapAsync(NativeAsync.ZRevRangeByScoreWithScoresAsync(setId, fromScore, toScore, skip, take, cancellationToken));

        ValueTask<long> IRedisClientAsync.RemoveRangeFromSortedSetAsync(string setId, int minRank, int maxRank, CancellationToken cancellationToken)
            => NativeAsync.ZRemRangeByRankAsync(setId, minRank, maxRank, cancellationToken);

        ValueTask<long> IRedisClientAsync.RemoveRangeFromSortedSetByScoreAsync(string setId, double fromScore, double toScore, CancellationToken cancellationToken)
            => NativeAsync.ZRemRangeByScoreAsync(setId, fromScore, toScore, cancellationToken);

        ValueTask<long> IRedisClientAsync.RemoveRangeFromSortedSetByScoreAsync(string setId, long fromScore, long toScore, CancellationToken cancellationToken)
            => NativeAsync.ZRemRangeByScoreAsync(setId, fromScore, toScore, cancellationToken);

        ValueTask<long> IRedisClientAsync.StoreIntersectFromSortedSetsAsync(string intoSetId, string[] setIds, CancellationToken cancellationToken)
            => NativeAsync.ZInterStoreAsync(intoSetId, setIds, cancellationToken);

        ValueTask<long> IRedisClientAsync.StoreIntersectFromSortedSetsAsync(string intoSetId, string[] setIds, string[] args, CancellationToken cancellationToken)
            => base.ZInterStoreAsync(intoSetId, setIds, args, cancellationToken);

        ValueTask<long> IRedisClientAsync.StoreUnionFromSortedSetsAsync(string intoSetId, string[] setIds, CancellationToken cancellationToken)
            => NativeAsync.ZUnionStoreAsync(intoSetId, setIds, cancellationToken);

        ValueTask<long> IRedisClientAsync.StoreUnionFromSortedSetsAsync(string intoSetId, string[] setIds, string[] args, CancellationToken cancellationToken)
            => base.ZUnionStoreAsync(intoSetId, setIds, args, cancellationToken);

        ValueTask<bool> IRedisClientAsync.HashContainsEntryAsync(string hashId, string key, CancellationToken cancellationToken)
            => NativeAsync.HExistsAsync(hashId, key.ToUtf8Bytes(), cancellationToken).IsSuccessAsync();

        ValueTask<bool> IRedisClientAsync.SetEntryInHashIfNotExistsAsync(string hashId, string key, string value, CancellationToken cancellationToken)
            => NativeAsync.HSetNXAsync(hashId, key.ToUtf8Bytes(), value.ToUtf8Bytes(), cancellationToken).IsSuccessAsync();

        ValueTask IRedisClientAsync.SetRangeInHashAsync(string hashId, IEnumerable<KeyValuePair<string, string>> keyValuePairs, CancellationToken cancellationToken)
            => SetRangeInHashPrepare(keyValuePairs, out var keys, out var values) ? NativeAsync.HMSetAsync(hashId, keys, values, cancellationToken) : default;

        ValueTask<long> IRedisClientAsync.IncrementValueInHashAsync(string hashId, string key, int incrementBy, CancellationToken cancellationToken)
            => NativeAsync.HIncrbyAsync(hashId, key.ToUtf8Bytes(), incrementBy, cancellationToken);

        ValueTask<double> IRedisClientAsync.IncrementValueInHashAsync(string hashId, string key, double incrementBy, CancellationToken cancellationToken)
            => NativeAsync.HIncrbyFloatAsync(hashId, key.ToUtf8Bytes(), incrementBy, cancellationToken);

        ValueTask<string> IRedisClientAsync.GetValueFromHashAsync(string hashId, string key, CancellationToken cancellationToken)
            => NativeAsync.HGetAsync(hashId, key.ToUtf8Bytes(), cancellationToken).FromUtf8BytesAsync();

        ValueTask<List<string>> IRedisClientAsync.GetValuesFromHashAsync(string hashId, string[] keys, CancellationToken cancellationToken)
        {
            if (keys.Length == 0) return new List<string>().AsValueTask();
            var keyBytes = ConvertToBytes(keys);
            return NativeAsync.HMGetAsync(hashId, keyBytes, cancellationToken).ToStringListAsync();
        }

        ValueTask<bool> IRedisClientAsync.RemoveEntryFromHashAsync(string hashId, string key, CancellationToken cancellationToken)
            => NativeAsync.HDelAsync(hashId, key.ToUtf8Bytes(), cancellationToken).IsSuccessAsync();

        ValueTask<List<string>> IRedisClientAsync.GetHashKeysAsync(string hashId, CancellationToken cancellationToken)
            => NativeAsync.HKeysAsync(hashId, cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.GetHashValuesAsync(string hashId, CancellationToken cancellationToken)
            => NativeAsync.HValsAsync(hashId, cancellationToken).ToStringListAsync();

        ValueTask<Dictionary<string, string>> IRedisClientAsync.GetAllEntriesFromHashAsync(string hashId, CancellationToken cancellationToken)
            => NativeAsync.HGetAllAsync(hashId, cancellationToken).Await(ret => ret.ToStringDictionary());

        ValueTask<RedisText> IRedisClientAsync.ExecLuaAsync(string body, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalCommandAsync(body, 0, args.ToMultiByteArray(), cancellationToken).Await(ret => ret.ToRedisText());

        ValueTask<RedisText> IRedisClientAsync.ExecLuaShaAsync(string sha1, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaCommandAsync(sha1, 0, args.ToMultiByteArray(), cancellationToken).Await(ret => ret.ToRedisText());

        ValueTask<string> IRedisClientAsync.ExecLuaAsStringAsync(string body, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalStrAsync(body, 0, args.ToMultiByteArray(), cancellationToken);

        ValueTask<string> IRedisClientAsync.ExecLuaShaAsStringAsync(string sha1, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaStrAsync(sha1, 0, args.ToMultiByteArray(), cancellationToken);

        ValueTask<long> IRedisClientAsync.ExecLuaAsIntAsync(string body, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalIntAsync(body, 0, args.ToMultiByteArray(), cancellationToken);

        ValueTask<long> IRedisClientAsync.ExecLuaAsIntAsync(string body, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalIntAsync(body, keys.Length, MergeAndConvertToBytes(keys, args), cancellationToken);

        ValueTask<long> IRedisClientAsync.ExecLuaShaAsIntAsync(string sha1, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaIntAsync(sha1, 0, args.ToMultiByteArray(), cancellationToken);

        ValueTask<long> IRedisClientAsync.ExecLuaShaAsIntAsync(string sha1, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaIntAsync(sha1, keys.Length, MergeAndConvertToBytes(keys, args), cancellationToken);

        ValueTask<List<string>> IRedisClientAsync.ExecLuaAsListAsync(string body, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalAsync(body, 0, args.ToMultiByteArray(), cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.ExecLuaAsListAsync(string body, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalAsync(body, keys.Length, MergeAndConvertToBytes(keys, args), cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.ExecLuaShaAsListAsync(string sha1, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaAsync(sha1, 0, args.ToMultiByteArray(), cancellationToken).ToStringListAsync();

        ValueTask<List<string>> IRedisClientAsync.ExecLuaShaAsListAsync(string sha1, string[] keys, string[] args, CancellationToken cancellationToken)
            => NativeAsync.EvalShaAsync(sha1, keys.Length, MergeAndConvertToBytes(keys, args), cancellationToken).ToStringListAsync();

        ValueTask<string> IRedisClientAsync.CalculateSha1Async(string luaBody, CancellationToken cancellationToken)
            => CalculateSha1(luaBody).AsValueTask();

        async ValueTask<bool> IRedisClientAsync.HasLuaScriptAsync(string sha1Ref, CancellationToken cancellationToken)
        {
            var map = await AsAsync().WhichLuaScriptsExistsAsync(new[] { sha1Ref }, cancellationToken).ConfigureAwait(false);
            return map[sha1Ref];
        }

        async ValueTask<Dictionary<string, bool>> IRedisClientAsync.WhichLuaScriptsExistsAsync(string[] sha1Refs, CancellationToken cancellationToken)
        {
            var intFlags = await NativeAsync.ScriptExistsAsync(sha1Refs.ToMultiByteArray()).ConfigureAwait(false);
            return WhichLuaScriptsExistsParseResult(sha1Refs, intFlags);
        }

        ValueTask IRedisClientAsync.RemoveAllLuaScriptsAsync(CancellationToken cancellationToken)
            => NativeAsync.ScriptFlushAsync(cancellationToken);

        ValueTask IRedisClientAsync.KillRunningLuaScriptAsync(CancellationToken cancellationToken)
            => NativeAsync.ScriptKillAsync(cancellationToken);

        ValueTask<RedisText> IRedisClientAsync.CustomAsync(params object[] cmdWithArgs)
            => AsAsync().CustomAsync(cmdWithArgs, cancellationToken: default);

        ValueTask<bool> IRedisClientAsync.RemoveEntryAsync(params string[] args)
            => AsAsync().RemoveEntryAsync(args, cancellationToken: default);

        ValueTask<bool> IRedisClientAsync.AddToHyperLogAsync(string key, params string[] elements)
            => AsAsync().AddToHyperLogAsync(key, elements, cancellationToken: default);

        ValueTask IRedisClientAsync.MergeHyperLogsAsync(string toKey, params string[] fromKeys)
            => AsAsync().MergeHyperLogsAsync(toKey, fromKeys, cancellationToken: default);

        ValueTask<long> IRedisClientAsync.AddGeoMembersAsync(string key, params RedisGeo[] geoPoints)
            => AsAsync().AddGeoMembersAsync(key, geoPoints, cancellationToken: default);

        ValueTask<string[]> IRedisClientAsync.GetGeohashesAsync(string key, params string[] members)
            => AsAsync().GetGeohashesAsync(key, members, cancellationToken: default);

        ValueTask<List<RedisGeo>> IRedisClientAsync.GetGeoCoordinatesAsync(string key, params string[] members)
            => AsAsync().GetGeoCoordinatesAsync(key, members, cancellationToken: default);

        ValueTask IRedisClientAsync.WatchAsync(params string[] keys)
            => AsAsync().WatchAsync(keys, cancellationToken: default);

        ValueTask<HashSet<string>> IRedisClientAsync.GetIntersectFromSetsAsync(params string[] setIds)
            => AsAsync().GetIntersectFromSetsAsync(setIds, cancellationToken: default);

        ValueTask IRedisClientAsync.StoreIntersectFromSetsAsync(string intoSetId, params string[] setIds)
            => AsAsync().StoreIntersectFromSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask<HashSet<string>> IRedisClientAsync.GetUnionFromSetsAsync(params string[] setIds)
            => AsAsync().GetUnionFromSetsAsync(setIds, cancellationToken: default);

        ValueTask IRedisClientAsync.StoreUnionFromSetsAsync(string intoSetId, params string[] setIds)
            => AsAsync().StoreUnionFromSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask<HashSet<string>> IRedisClientAsync.GetDifferencesFromSetAsync(string fromSetId, params string[] withSetIds)
            => AsAsync().GetDifferencesFromSetAsync(fromSetId, withSetIds, cancellationToken: default);

        ValueTask IRedisClientAsync.StoreDifferencesFromSetAsync(string intoSetId, string fromSetId, params string[] withSetIds)
            => AsAsync().StoreDifferencesFromSetAsync(intoSetId, fromSetId, withSetIds, cancellationToken: default);

        ValueTask<long> IRedisClientAsync.StoreIntersectFromSortedSetsAsync(string intoSetId, params string[] setIds)
            => AsAsync().StoreIntersectFromSortedSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask<long> IRedisClientAsync.StoreUnionFromSortedSetsAsync(string intoSetId, params string[] setIds)
            => AsAsync().StoreUnionFromSortedSetsAsync(intoSetId, setIds, cancellationToken: default);

        ValueTask<List<string>> IRedisClientAsync.GetValuesFromHashAsync(string hashId, params string[] keys)
            => AsAsync().GetValuesFromHashAsync(hashId, keys, cancellationToken: default);

        ValueTask<RedisText> IRedisClientAsync.ExecLuaAsync(string body, params string[] args)
            => AsAsync().ExecLuaAsync(body, args, cancellationToken: default);

        ValueTask<RedisText> IRedisClientAsync.ExecLuaShaAsync(string sha1, params string[] args)
            => AsAsync().ExecLuaShaAsync(sha1, args, cancellationToken: default);

        ValueTask<string> IRedisClientAsync.ExecLuaAsStringAsync(string luaBody, params string[] args)
            => AsAsync().ExecLuaAsStringAsync(luaBody, args, cancellationToken: default);

        ValueTask<string> IRedisClientAsync.ExecLuaShaAsStringAsync(string sha1, params string[] args)
            => AsAsync().ExecLuaShaAsStringAsync(sha1, args, cancellationToken: default);

        ValueTask<long> IRedisClientAsync.ExecLuaAsIntAsync(string luaBody, params string[] args)
            => AsAsync().ExecLuaAsIntAsync(luaBody, args, cancellationToken: default);

        ValueTask<long> IRedisClientAsync.ExecLuaShaAsIntAsync(string sha1, params string[] args)
            => AsAsync().ExecLuaShaAsIntAsync(sha1, args, cancellationToken: default);

        ValueTask<List<string>> IRedisClientAsync.ExecLuaAsListAsync(string luaBody, params string[] args)
            => AsAsync().ExecLuaAsListAsync(luaBody, args, cancellationToken: default);

        ValueTask<List<string>> IRedisClientAsync.ExecLuaShaAsListAsync(string sha1, params string[] args)
            => AsAsync().ExecLuaShaAsListAsync(sha1, args, cancellationToken: default);

        ValueTask<Dictionary<string, bool>> IRedisClientAsync.WhichLuaScriptsExistsAsync(params string[] sha1Refs)
            => AsAsync().WhichLuaScriptsExistsAsync(sha1Refs, cancellationToken: default);
    }
}
