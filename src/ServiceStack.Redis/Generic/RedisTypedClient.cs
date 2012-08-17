//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2010 Liquidbit Ltd.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ServiceStack.Common.Extensions;
using ServiceStack.Common.Utils;
using ServiceStack.DesignPatterns.Model;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Text;

namespace ServiceStack.Redis.Generic
{
	/// <summary>
	/// Allows you to get Redis value operations to operate against POCO types.
	/// </summary>
	/// <typeparam name="T"></typeparam>
	public partial class RedisTypedClient<T>
		: IRedisTypedClient<T>
	{
		readonly ITypeSerializer<T> serializer = new JsonSerializer<T>();
		private readonly RedisClient client;

        public IRedisClient RedisClient
        {
            get { return client; }
        }

		public IRedisNativeClient NativeClient
		{
			get { return client; }
		}

		/// <summary>
		/// Use this to share the same redis connection with another
		/// </summary>
		/// <param name="client">The client.</param>
		public RedisTypedClient(RedisClient client)
		{
			this.client = client;
			this.Lists = new RedisClientLists(this);
			this.Sets = new RedisClientSets(this);
			this.SortedSets = new RedisClientSortedSets(this);

			this.SequenceKey = client.GetTypeSequenceKey<T>();
			this.TypeIdsSetKey = client.GetTypeIdsSetKey<T>();
			this.TypeLockKey = string.Concat(client.NamespacePrefix, "lock:", typeof(T).Name);
            this.RecentSortedSetKey = string.Concat(client.NamespacePrefix, "recent:", typeof(T).Name);
		}

        private readonly string RecentSortedSetKey;
		public string TypeIdsSetKey { get; set; }
		public string TypeLockKey { get; set; }

		public IRedisTypedTransaction<T> CreateTransaction()
		{
			return new RedisTypedTransaction<T>(this);
		}

        public IRedisTypedPipeline<T> CreatePipeline()
        {
            return new RedisTypedPipeline<T>(this);
        }
		public IDisposable AcquireLock()
		{
			return client.AcquireLock(this.TypeLockKey);
		}

		public IDisposable AcquireLock(TimeSpan timeOut)
		{
			return client.AcquireLock(this.TypeLockKey, timeOut);
		}

		public IRedisTransactionBase Transaction
		{
			get
			{
				return client.Transaction;
			}
			set
			{
				client.Transaction = value;
			}
		}

        public IRedisPipelineShared Pipeline
        {
            get
            {
                return client.Pipeline;
            }
            set
            {
                client.Pipeline = value;
            }
        }

        public void Watch(params string[] keys)
        {
            client.Watch(keys);
        }

        public void UnWatch()
        {
            client.UnWatch();
        }

		public void Multi()
		{
			this.client.Multi();
		}

		public void Discard()
		{
			this.client.Discard();
		}

		public void Exec()
		{
			client.Exec();
		}

		internal void AddTypeIdsRegisteredDuringPipeline()
		{
			client.AddTypeIdsRegisteredDuringPipeline();
		}

		internal void ClearTypeIdsRegisteredDuringPipeline()
		{
			client.ClearTypeIdsRegisteredDuringPipeline();
		}

		public List<string> GetAllKeys()
		{
			return client.GetAllKeys();
		}

	    public IRedisSet TypeIdsSet
		{
			get
			{
				return new RedisClientSet(client, client.GetTypeIdsSetKey<T>());
			}
		}

		public T this[string key]
		{
			get { return GetValue(key); }
			set { SetEntry(key, value); }
		}

		public byte[] SerializeValue(T value)
		{
			var strValue = serializer.SerializeToString(value);
			return Encoding.UTF8.GetBytes(strValue);
		}

		public T DeserializeValue(byte[] value)
		{
			var strValue = value != null ? Encoding.UTF8.GetString(value) : null;
			return serializer.DeserializeFromString(strValue);
		}

		public void SetEntry(string key, T value)
		{
			if (key == null)
				throw new ArgumentNullException("key");

			client.Set(key, SerializeValue(value));
			client.RegisterTypeId(value);
		}

		public void SetEntry(string key, T value, TimeSpan expireIn)
		{
			if (key == null)
				throw new ArgumentNullException("key");

			client.Set(key, SerializeValue(value), expireIn);
			client.RegisterTypeId(value);
		}

		public bool SetEntryIfNotExists(string key, T value)
		{
			var success = client.SetNX(key, SerializeValue(value)) == RedisNativeClient.Success;
			if (success) client.RegisterTypeId(value);
			return success;
		}

		public T GetValue(string key)
		{
			return DeserializeValue(client.Get(key));
		}

		public T GetAndSetValue(string key, T value)
		{
			return DeserializeValue(client.GetSet(key, SerializeValue(value)));
		}

		public bool ContainsKey(string key)
		{
			return client.Exists(key) == RedisNativeClient.Success;
		}

		public bool RemoveEntry(string key)
		{
			return client.Del(key) == RedisNativeClient.Success;
		}

		public bool RemoveEntry(params string[] keys)
		{
			return client.Del(keys) == RedisNativeClient.Success;
		}

		public bool RemoveEntry(params IHasStringId[] entities)
		{
			var ids = entities.ConvertAll(x => x.Id);
			var success = client.Del(ids.ToArray()) == RedisNativeClient.Success;
			if (success) client.RemoveTypeIds(ids.ToArray());
			return success;
		}

		public long IncrementValue(string key)
		{
			return client.Incr(key);
		}

		public long IncrementValueBy(string key, int count)
		{
			return client.IncrBy(key, count);
		}

		public long DecrementValue(string key)
		{
			return client.Decr(key);
		}

		public long DecrementValueBy(string key, int count)
		{
			return client.DecrBy(key, count);
		}

		public string SequenceKey { get; set; }

		public void SetSequence(int value)
		{
			client.GetSet(SequenceKey, Encoding.UTF8.GetBytes(value.ToString()));
		}

		public long GetNextSequence()
		{
			return IncrementValue(SequenceKey);
		}

		public long GetNextSequence(int incrBy)
		{
			return IncrementValueBy(SequenceKey, incrBy);
		}

		public RedisKeyType GetEntryType(string key)
		{
			return client.GetEntryType(key);
		}

		public string GetRandomKey()
		{
			return client.RandomKey();
		}

		public bool ExpireEntryIn(string key, TimeSpan expireIn)
		{
			return client.Expire(key, (int)expireIn.TotalSeconds);
		}

		public bool ExpireEntryAt(string key, DateTime expireAt)
		{
			return client.ExpireAt(key, expireAt.ToUnixTime());
		}

		public bool ExpireIn(object id, TimeSpan expireIn)
		{
			var key = client.UrnKey<T>(id);
			return client.Expire(key, (int)expireIn.TotalSeconds);
		}

		public bool ExpireAt(object id, DateTime expireAt)
		{
			var key = client.UrnKey<T>(id);
			return client.ExpireAt(key, expireAt.ToUnixTime());
		}

		public TimeSpan GetTimeToLive(string key)
		{
			return TimeSpan.FromSeconds(client.Ttl(key));
		}

		public void Save()
		{
			client.Save();
		}

		public void SaveAsync()
		{
			client.SaveAsync();
		}

		public void FlushDb()
		{
			client.FlushDb();
		}

		public void FlushAll()
		{
			client.FlushAll();
		}

		public T[] SearchKeys(string pattern)
		{
			var strKeys = client.SearchKeys(pattern);
			var keysCount = strKeys.Count;

			var keys = new T[keysCount];
			for (var i=0; i < keysCount; i++)
			{
				keys[i] = serializer.DeserializeFromString(strKeys[i]);
			}
			return keys;
		}

		public List<T> GetValues(List<string> keys)
		{
			if (keys.IsNullOrEmpty()) return  new List<T>();

			var resultBytesArray = client.MGet(keys.ToArray());

			var results = new List<T>();
			foreach (var resultBytes in resultBytesArray)
			{
				if (resultBytes == null) continue;

				var result = DeserializeValue(resultBytes);
				results.Add(result);
			}

			return results;
		}

        public void StoreAsHash(T entity)
        {
            client.StoreAsHash(entity);
        }

        public T GetFromHash(object id)
        {
            return client.GetFromHash<T>(id);
        }


		#region Implementation of IBasicPersistenceProvider<T>

		public T GetById(object id)
		{
			var key = client.UrnKey<T>(id);
			return this.GetValue(key);
		}

		public IList<T> GetByIds(IEnumerable ids)
		{
			if (ids != null)
			{
                var urnKeys = ids.ConvertAll(x => client.UrnKey<T>(x));
				if (urnKeys.Count != 0)
					return GetValues(urnKeys);
			}

			return new List<T>();
		}

		public IList<T> GetAll()
		{
			var allKeys = client.GetAllItemsFromSet(this.TypeIdsSetKey);
			return this.GetByIds(allKeys.ToArray());
		}

		public T Store(T entity)
		{
            var urnKey = client.UrnKey(entity);
			this.SetEntry(urnKey, entity);

			return entity;
		}

		public void StoreAll(IEnumerable<T> entities)
		{
			if (entities == null) return;

			var entitiesList = entities.ToList();
			var len = entitiesList.Count;

			var keys = new byte[len][];
			var values = new byte[len][];

			for (var i = 0; i < len; i++)
			{
                keys[i] = client.UrnKey(entitiesList[i]).ToUtf8Bytes();
				values[i] = Redis.RedisClient.SerializeToUtf8Bytes(entitiesList[i]);
			}

			client.MSet(keys, values);
			client.RegisterTypeIds(entitiesList);
		}

		public void Delete(T entity)
		{
            var urnKey = client.UrnKey(entity);
			this.RemoveEntry(urnKey);
			client.RemoveTypeIds(entity);
		}

		public void DeleteById(object id)
		{
			var urnKey = client.UrnKey<T>(id);

			this.RemoveEntry(urnKey);
			client.RemoveTypeIds<T>(id.ToString());
		}

		public void DeleteByIds(IEnumerable ids)
		{
			if (ids == null) return;

			var urnKeys = ids.ConvertAll(t => client.UrnKey<T>(t));
			this.RemoveEntry(urnKeys.ToArray());
			client.RemoveTypeIds<T>(ids.ConvertAll(x => x.ToString()).ToArray());
		}

		public void DeleteAll()
		{
			var urnKeys = client.GetAllItemsFromSet(this.TypeIdsSetKey);
			this.RemoveEntry(urnKeys.ToArray());
			this.RemoveEntry(this.TypeIdsSetKey);
		}

		#endregion

        internal void ExpectQueued()
        {
            client.ExpectQueued();
        }
        internal void ExpectOk()
        {
            client.ExpectOk();
        }
        internal int ReadMultiDataResultCount()
        {
            return client.ReadMultiDataResultCount();
        }
        public void FlushSendBuffer()
        {
            client.FlushSendBuffer();
        }
        public void ResetSendBuffer()
        {
            client.ResetSendBuffer();
        }

        [Obsolete("Does nothing currently, RedisTypedClient will not be IDisposable in a future version")]
	    public void Dispose() {}
	}
}