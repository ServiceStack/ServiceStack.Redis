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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ServiceStack.Redis.Generic;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    /// <summary>
    /// The client wraps the native redis operations into a more readable c# API.
    /// 
    /// Where possible these operations are also exposed in common c# interfaces, 
    /// e.g. RedisClient.Lists => IList[string]
    ///		 RedisClient.Sets => ICollection[string]
    /// </summary>
    public partial class RedisClient
        : RedisNativeClient, IRedisClient
    {
        public RedisClient()
        {
            Init();
        }

        internal static HashSet<Type> __uniqueTypes = new HashSet<Type>();

        public static Func<RedisClient> NewFactoryFn = () => new RedisClient();

        /// <summary>
        /// Creates a new instance of the Redis Client from NewFactoryFn. 
        /// </summary>
        public static RedisClient New()
        {
            return NewFactoryFn();
        }

        public RedisClient(string host)
            : base(host)
        {
            Init();
        }

        public RedisClient(string host, int port)
            : base(host, port)
        {
            Init();
        }

        public RedisClient(string host, int port, string password = null, long db = DefaultDb)
            : base(host, port, password, db)
        {
            Init();
        }

        public RedisClient(Uri uri)
            : base(uri.Host, uri.Port)
        {
            var password = !String.IsNullOrEmpty(uri.UserInfo) ? uri.UserInfo.Split(':')[1] : null;
            Password = password;
            Init();
        }

        public void Init()
        {
            this.Lists = new RedisClientLists(this);
            this.Sets = new RedisClientSets(this);
            this.SortedSets = new RedisClientSortedSets(this);
            this.Hashes = new RedisClientHashes(this);
        }

        public string this[string key]
        {
            get { return GetValue(key); }
            set { SetEntry(key, value); }
        }

        public override void OnConnected() {}

        public DateTime ConvertToServerDate(DateTime expiresAt)
        {
            //placeholder if we ever try to compensate for differences in server-time
            return expiresAt;
        }

        public string GetTypeSequenceKey<T>()
        {
            return String.Concat(NamespacePrefix, "seq:", typeof(T).Name);
        }

        public string GetTypeIdsSetKey<T>()
        {
            return String.Concat(NamespacePrefix, "ids:", typeof(T).Name);
        }

        public string GetTypeIdsSetKey(Type type)
        {
            return String.Concat(NamespacePrefix, "ids:", type.Name);
        }

        public void RewriteAppendOnlyFileAsync()
        {
            base.BgRewriteAof();
        }

        public List<string> GetAllKeys()
        {
            return SearchKeys("*");
        }

        public void SetEntry(string key, string value)
        {
            var bytesValue = value != null
                ? value.ToUtf8Bytes()
                : null;

            base.Set(key, bytesValue);
        }

        public void SetEntry(string key, string value, TimeSpan expireIn)
        {
            var bytesValue = value != null
                ? value.ToUtf8Bytes()
                : null;

            if (ServerVersionNumber >= 2600)
            {
                if (expireIn.Milliseconds > 0)
                    base.Set(key, bytesValue, 0, (long)expireIn.TotalMilliseconds);
                else
                    base.Set(key, bytesValue, (int)expireIn.TotalSeconds, 0);
            }
            else
            {
                SetEx(key, (int)expireIn.TotalSeconds, bytesValue);
            }
        }

        public void SetEntryIfExists(string key, string value, TimeSpan expireIn)
        {
            var bytesValue = value != null ? value.ToUtf8Bytes() : null;

            if (expireIn.Milliseconds > 0)
                base.Set(key, bytesValue, 0, (long)expireIn.TotalMilliseconds, exists: true);
            else
                base.Set(key, bytesValue, (int)expireIn.TotalSeconds, 0, exists: true);
        }

        public bool SetEntryIfNotExists(string key, string value)
        {
            if (value == null)
                throw new ArgumentNullException("value");

            return SetNX(key, value.ToUtf8Bytes()) == Success;
        }

        public void SetEntryIfNotExists(string key, string value, TimeSpan expireIn)
        {
            var bytesValue = value != null ? value.ToUtf8Bytes() : null;

            if (expireIn.Milliseconds > 0)
                base.Set(key, bytesValue, 0, (long)expireIn.TotalMilliseconds, exists: false);
            else
                base.Set(key, bytesValue, (int)expireIn.TotalSeconds, 0, exists: false);
        }

        public void ChangeDb(long db)
        {
            Db = db;
            SendExpectSuccess(Commands.Select, db.ToUtf8Bytes());
        }

        public List<Dictionary<string, string>> GetClientList()
        {
            var clientList = base.ClientList().FromUtf8Bytes();
            var results = new List<Dictionary<string, string>>();

            var lines = clientList.Split('\n');
            foreach (var line in lines)
            {
                if (String.IsNullOrEmpty(line)) continue;

                var map = new Dictionary<string, string>();
                var parts = line.Split(' ');
                foreach (var part in parts)
                {
                    var keyValue = part.SplitOnFirst('=');
                    map[keyValue[0]] = keyValue[1];
                }
                results.Add(map);
            }
            return results;
        }

        public void SetAll(IEnumerable<string> keys, IEnumerable<string> values)
        {
            if (keys == null || values == null) return;
            var keyArray = keys.ToArray();
            var valueArray = values.ToArray();

            if (keyArray.Length != valueArray.Length)
                throw new Exception("Key length != Value Length. {0}/{1}".Fmt(keyArray.Length, valueArray.Length));

            if (keyArray.Length == 0) return;

            var keyBytes = new byte[keyArray.Length][];
            var valBytes = new byte[keyArray.Length][];
            for (int i = 0; i < keyArray.Length; i++)
            {
                keyBytes[i] = keyArray[i].ToUtf8Bytes();
                valBytes[i] = valueArray[i].ToUtf8Bytes();
            }

            base.MSet(keyBytes, valBytes);
        }

        public void SetAll(Dictionary<string, string> map)
        {
            if (map == null || map.Count == 0) return;

            var keyBytes = new byte[map.Count][];
            var valBytes = new byte[map.Count][];

            var i = 0;
            foreach (var key in map.Keys)
            {
                var val = map[key];
                keyBytes[i] = key.ToUtf8Bytes();
                valBytes[i] = val.ToUtf8Bytes();
                i++;
            }

            base.MSet(keyBytes, valBytes);
        }

        public string GetValue(string key)
        {
            var bytes = Get(key);
            return bytes == null
                ? null
                : bytes.FromUtf8Bytes();
        }

        public string GetAndSetEntry(string key, string value)
        {
            return GetSet(key, value.ToUtf8Bytes()).FromUtf8Bytes();
        }

        public bool ContainsKey(string key)
        {
            return Exists(key) == Success;
        }

        public bool Remove(string key)
        {
            return Del(key) == Success;
        }

        public bool RemoveEntry(params string[] keys)
        {
            if (keys.Length == 0) return false;

            return Del(keys) == Success;
        }

        public long IncrementValue(string key)
        {
            return Incr(key);
        }

        public long IncrementValueBy(string key, int count)
        {
            return IncrBy(key, count);
        }

        public long IncrementValueBy(string key, long count)
        {
            return IncrBy(key, count);
        }
        
        public double IncrementValueBy(string key, double count)
        {
            return IncrByFloat(key, count);
        }

        public long DecrementValue(string key)
        {
            return Decr(key);
        }

        public long DecrementValueBy(string key, int count)
        {
            return DecrBy(key, count);
        }

        public long AppendToValue(string key, string value)
        {
            return base.Append(key, value.ToUtf8Bytes());
        }

        public void RenameKey(string fromName, string toName)
        {
            base.Rename(fromName, toName);
        }

        public string GetRandomKey()
        {
            return RandomKey();
        }

        public bool ExpireEntryIn(string key, TimeSpan expireIn)
        {
            if (ServerVersionNumber >= 2600)
            {
                if (expireIn.Milliseconds > 0)
                {
                    return PExpire(key, (long)expireIn.TotalMilliseconds);
                }
            }
 
            return Expire(key, (int)expireIn.TotalSeconds);
        }

        public bool ExpireEntryAt(string key, DateTime expireAt)
        {
            if (ServerVersionNumber >= 2600)
            {
                return PExpireAt(key, ConvertToServerDate(expireAt).ToUnixTimeMs());
            }
            else
            {
                return ExpireAt(key, ConvertToServerDate(expireAt).ToUnixTime());
            }
        }

        public TimeSpan GetTimeToLive(string key)
        {
            return TimeSpan.FromSeconds(Ttl(key));
        }

        public void SetConfig(string configItem, string value)
        {
            base.ConfigSet(configItem, value.ToUtf8Bytes());
        }

        public string GetConfig(string configItem)
        {
            var sb = new StringBuilder();
            var byteArray = base.ConfigGet(configItem);
            foreach (var bytes in byteArray)
            {
                if (sb.Length > 0)
                    sb.Append(" ");

                sb.Append(bytes.FromUtf8Bytes());
            }
            return sb.ToString();
        }

        public DateTime GetServerTime()
        {
            var parts = base.Time();
            var unixTime = long.Parse(parts[0].FromUtf8Bytes());
            var microSecs = long.Parse(parts[1].FromUtf8Bytes());
            var ticks = microSecs / 1000 * TimeSpan.TicksPerMillisecond;

            var date = unixTime.FromUnixTime();
            var timeSpan = TimeSpan.FromTicks(ticks);
            return date + timeSpan;
        }

        public IRedisTypedClient<T> As<T>()
        {
            try
            {
                var typedClient = new RedisTypedClient<T>(this);
                LicenseUtils.AssertValidUsage(LicenseFeature.Redis, QuotaType.Types, __uniqueTypes.Count);
                return typedClient;
            }
            catch (TypeInitializationException ex)
            {
                throw ex.GetInnerMostException();
            }
        }

        public IDisposable AcquireLock(string key)
        {
            return new RedisLock(this, key, null);
        }

        public IDisposable AcquireLock(string key, TimeSpan timeOut)
        {
            return new RedisLock(this, key, timeOut);
        }

        public IRedisTransaction CreateTransaction()
        {
            return new RedisTransaction(this);
        }

        public IRedisPipeline CreatePipeline()
        {
            return new RedisAllPurposePipeline(this);
        }

        public List<string> SearchKeys(string pattern)
        {
            var multiDataList = Keys(pattern);
            return multiDataList.ToStringList();
        }

        public List<string> GetValues(List<string> keys)
        {
            if (keys == null) throw new ArgumentNullException("keys");
            if (keys.Count == 0) return new List<string>();

            var resultBytesArray = MGet(keys.ToArray());

            var results = new List<string>();
            foreach (var resultBytes in resultBytesArray)
            {
                if (resultBytes == null) continue;

                var resultString = resultBytes.FromUtf8Bytes();
                results.Add(resultString);
            }

            return results;
        }

        public List<T> GetValues<T>(List<string> keys)
        {
            if (keys == null) throw new ArgumentNullException("keys");
            if (keys.Count == 0) return new List<T>();

            var resultBytesArray = MGet(keys.ToArray());

            var results = new List<T>();
            foreach (var resultBytes in resultBytesArray)
            {
                if (resultBytes == null) continue;

                var resultString = resultBytes.FromUtf8Bytes();
                var result = resultString.FromJson<T>();
                results.Add(result);
            }

            return results;
        }

        public Dictionary<string, string> GetValuesMap(List<string> keys)
        {
            if (keys == null) throw new ArgumentNullException("keys");
            if (keys.Count == 0) return new Dictionary<string, string>();

            var keysArray = keys.ToArray();
            var resultBytesArray = MGet(keysArray);

            var results = new Dictionary<string, string>();
            for (var i = 0; i < resultBytesArray.Length; i++)
            {
                var key = keysArray[i];

                var resultBytes = resultBytesArray[i];
                if (resultBytes == null)
                {
                    results.Add(key, null);
                }
                else
                {
                    var resultString = resultBytes.FromUtf8Bytes();
                    results.Add(key, resultString);
                }
            }

            return results;
        }

        public Dictionary<string, T> GetValuesMap<T>(List<string> keys)
        {
            if (keys == null) throw new ArgumentNullException("keys");
            if (keys.Count == 0) return new Dictionary<string, T>();

            var keysArray = keys.ToArray();
            var resultBytesArray = MGet(keysArray);

            var results = new Dictionary<string, T>();
            for (var i = 0; i < resultBytesArray.Length; i++)
            {
                var key = keysArray[i];

                var resultBytes = resultBytesArray[i];
                if (resultBytes == null)
                {
                    results.Add(key, default(T));
                }
                else
                {
                    var resultString = resultBytes.FromUtf8Bytes();
                    var result = JsonSerializer.DeserializeFromString<T>(resultString);
                    results.Add(key, result);
                }
            }

            return results;
        }

        public IRedisSubscription CreateSubscription()
        {
            return new RedisSubscription(this);
        }

        public long PublishMessage(string toChannel, string message)
        {
            return base.Publish(toChannel, message.ToUtf8Bytes());
        }

        #region IBasicPersistenceProvider


        Dictionary<string, HashSet<string>> registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();

        internal HashSet<string> GetRegisteredTypeIdsWithinPipeline(string typeIdsSet)
        {
            HashSet<string> registeredTypeIdsWithinPipeline;
            if (!registeredTypeIdsWithinPipelineMap.TryGetValue(typeIdsSet, out registeredTypeIdsWithinPipeline))
            {
                registeredTypeIdsWithinPipeline = new HashSet<string>();
                registeredTypeIdsWithinPipelineMap[typeIdsSet] = registeredTypeIdsWithinPipeline;
            }
            return registeredTypeIdsWithinPipeline;
        }

        internal void RegisterTypeId<T>(T value)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            var id = value.GetId().ToString();

            RegisterTypeId(typeIdsSetKey, id);
        }

        internal void RegisterTypeId(string typeIdsSetKey, string id)
        {
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                registeredTypeIdsWithinPipeline.Add(id);
            }
            else
            {
                this.AddItemToSet(typeIdsSetKey, id);
            }
        }

        internal void RegisterTypeIds<T>(IEnumerable<T> values)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            var ids = values.Map(x => x.GetId().ToString());

            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                ids.ForEach(x => registeredTypeIdsWithinPipeline.Add(x));
            }
            else
            {
                AddRangeToSet(typeIdsSetKey, ids);
            }
        }

        internal void RemoveTypeIds<T>(params string[] ids)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                ids.Each(x => registeredTypeIdsWithinPipeline.Remove(x));
            }
            else
            {
                ids.Each(x => this.RemoveItemFromSet(typeIdsSetKey, x));
            }
        }

        internal void RemoveTypeIds<T>(params T[] values)
        {
            var typeIdsSetKey = GetTypeIdsSetKey<T>();
            if (this.Pipeline != null)
            {
                var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                values.Each(x => registeredTypeIdsWithinPipeline.Remove(x.GetId().ToString()));
            }
            else
            {
                values.Each(x => this.RemoveItemFromSet(typeIdsSetKey, x.GetId().ToString()));
            }
        }

        internal void AddTypeIdsRegisteredDuringPipeline()
        {
            foreach (var entry in registeredTypeIdsWithinPipelineMap)
            {
                var typeIdsSetKey = entry.Key;
                foreach (var id in entry.Value)
                {
                    var registeredTypeIdsWithinPipeline = GetRegisteredTypeIdsWithinPipeline(typeIdsSetKey);
                    registeredTypeIdsWithinPipeline.Each(x => this.AddItemToSet(typeIdsSetKey, id));
                }
            }
            registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();
        }

        internal void ClearTypeIdsRegisteredDuringPipeline()
        {
            registeredTypeIdsWithinPipelineMap = new Dictionary<string, HashSet<string>>();
        }


        public T GetById<T>(object id)
        {
            var key = UrnKey<T>(id);
            var valueString = this.GetValue(key);
            var value = JsonSerializer.DeserializeFromString<T>(valueString);
            return value;
        }

        public IList<T> GetByIds<T>(ICollection ids)
        {
            if (ids == null || ids.Count == 0)
                return new List<T>();

            var urnKeys = ids.Cast<object>().Map(UrnKey<T>);
            return GetValues<T>(urnKeys);
        }

        public IList<T> GetAll<T>()
        {
            var typeIdsSetKy = this.GetTypeIdsSetKey<T>();
            var allTypeIds = this.GetAllItemsFromSet(typeIdsSetKy);
            var urnKeys = allTypeIds.Cast<object>().Map(UrnKey<T>);
            return GetValues<T>(urnKeys);
        }

        public T Store<T>(T entity)
        {
            var urnKey = UrnKey(entity);
            var valueString = JsonSerializer.SerializeToString(entity);

            this.SetEntry(urnKey, valueString);
            RegisterTypeId(entity);

            return entity;
        }

        public object StoreObject(object entity)
        {
            if (entity == null) throw new ArgumentNullException("entity");

            var id = entity.GetObjectId();
            var entityType = entity.GetType();
            var urnKey = UrnKey(entityType, id);
            var valueString = JsonSerializer.SerializeToString(entity);

            this.SetEntry(urnKey, valueString);

            RegisterTypeId(GetTypeIdsSetKey(entityType), id.ToString());

            return entity;
        }

        public void StoreAll<TEntity>(IEnumerable<TEntity> entities)
        {
            _StoreAll(entities);
        }

        public T GetFromHash<T>(object id)
        {
            var key = UrnKey<T>(id);
            return
                GetAllEntriesFromHash(key).ToJson().FromJson<T>();
        }

        public void StoreAsHash<T>(T entity)
        {
            var key = UrnKey(entity);
            SetRangeInHash(key, entity.ToJson().FromJson<Dictionary<string, string>>());
            RegisterTypeId(entity);
        }

        //Without the Generic Constraints
        internal void _StoreAll<TEntity>(IEnumerable<TEntity> entities)
        {
            if (entities == null) return;

            var entitiesList = entities.ToList();
            var len = entitiesList.Count;
            if (len == 0) return;

            var keys = new byte[len][];
            var values = new byte[len][];

            for (var i = 0; i < len; i++)
            {
                keys[i] = UrnKey(entitiesList[i]).ToUtf8Bytes();
                values[i] = SerializeToUtf8Bytes(entitiesList[i]);
            }

            base.MSet(keys, values);
            RegisterTypeIds(entitiesList);
        }

        public void WriteAll<TEntity>(IEnumerable<TEntity> entities)
        {
            if (entities == null) return;

            var entitiesList = entities.ToList();
            var len = entitiesList.Count;

            var keys = new byte[len][];
            var values = new byte[len][];

            for (var i = 0; i < len; i++)
            {
                keys[i] = UrnKey(entitiesList[i]).ToUtf8Bytes();
                values[i] = SerializeToUtf8Bytes(entitiesList[i]);
            }

            base.MSet(keys, values);
        }

        public static byte[] SerializeToUtf8Bytes<T>(T value)
        {
            return Encoding.UTF8.GetBytes(JsonSerializer.SerializeToString(value));
        }

        public void Delete<T>(T entity)
        {
            var urnKey = UrnKey(entity);
            this.Remove(urnKey);
            this.RemoveTypeIds(entity);
        }

        public void DeleteById<T>(object id)
        {
            var urnKey = UrnKey<T>(id);
            this.Remove(urnKey);
            this.RemoveTypeIds<T>(id.ToString());
        }

        public void DeleteByIds<T>(ICollection ids)
        {
            if (ids == null || ids.Count == 0) return;

            var idsList = ids.Cast<object>();
            var urnKeys = idsList.Map(UrnKey<T>);
            this.RemoveEntry(urnKeys.ToArray());
            this.RemoveTypeIds<T>(idsList.Map(x => x.ToString()).ToArray());
        }

        public void DeleteAll<T>()
        {
            var typeIdsSetKey = this.GetTypeIdsSetKey<T>();
            var ids = this.GetAllItemsFromSet(typeIdsSetKey);
            if (ids.Count > 0)
            {
                var urnKeys = ids.ToList().ConvertAll(UrnKey<T>);
                this.RemoveEntry(urnKeys.ToArray());
                this.Remove(typeIdsSetKey);
            }
        }

        public RedisClient CloneClient()
        {
            return new RedisClient(Host, Port, Password, Db)
            {
                SendTimeout = SendTimeout,
                ReceiveTimeout = ReceiveTimeout
            };
        }

        /// <summary>
        /// Returns key with automatic object id detection in provided value with <typeparam name="T">generic type</typeparam>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal string UrnKey<T>(T value)
        {
            return String.Concat(NamespacePrefix, value.CreateUrn());
        }

        /// <summary>
        /// Returns key with explicit object id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        internal string UrnKey<T>(object id)
        {
            return String.Concat(NamespacePrefix, IdUtils.CreateUrn<T>(id));
        }

        /// <summary>
        /// Returns key with explicit object type and id.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        internal string UrnKey(Type type, object id)
        {
            return String.Concat(NamespacePrefix, IdUtils.CreateUrn(type, id));
        }


        #endregion

        #region LUA EVAL

        public long ExecLuaAsInt(string body, params string[] args)
        {
            return base.EvalInt(body, 0, args.ToMultiByteArray());
        }

        public long ExecLuaAsInt(string luaBody, string[] keys, string[] args)
        {
            return base.EvalInt(luaBody, keys.Length, MergeAndConvertToBytes(keys, args));
        }

        public long ExecLuaShaAsInt(string sha1, params string[] args)
        {
            return base.EvalShaInt(sha1, args.Length, args.ToMultiByteArray());
        }

        public long ExecLuaShaAsInt(string sha1, string[] keys, string[] args)
        {
            return base.EvalShaInt(sha1, keys.Length, MergeAndConvertToBytes(keys, args));
        }

        public string ExecLuaAsString(string body, params string[] args)
        {
            return base.EvalStr(body, 0, args.ToMultiByteArray());
        }

        public string ExecLuaAsString(string sha1, string[] keys, string[] args)
        {
            return base.EvalStr(sha1, keys.Length, MergeAndConvertToBytes(keys, args));
        }

        public string ExecLuaShaAsString(string sha1, params string[] args)
        {
            return base.EvalShaStr(sha1, 0, args.ToMultiByteArray());
        }

        public string ExecLuaShaAsString(string sha1, string[] keys, string[] args)
        {
            return base.EvalShaStr(sha1, keys.Length, MergeAndConvertToBytes(keys, args));
        }

        public List<string> ExecLuaAsList(string body, params string[] args)
        {
            return base.Eval(body, 0, args.ToMultiByteArray()).ToStringList();
        }

        public List<string> ExecLuaAsList(string luaBody, string[] keys, string[] args)
        {
            return base.Eval(luaBody, keys.Length, MergeAndConvertToBytes(keys, args)).ToStringList();
        }

        public List<string> ExecLuaShaAsList(string sha1, params string[] args)
        {
            return base.EvalSha(sha1, 0, args.ToMultiByteArray()).ToStringList();
        }

        public List<string> ExecLuaShaAsList(string sha1, string[] keys, string[] args)
        {
            return base.EvalSha(sha1, keys.Length, MergeAndConvertToBytes(keys, args)).ToStringList();
        }


        public bool HasLuaScript(string sha1Ref)
        {
            return WhichLuaScriptsExists(sha1Ref)[sha1Ref];
        }

        public Dictionary<string, bool> WhichLuaScriptsExists(params string[] sha1Refs)
        {
            var intFlags = base.ScriptExists(sha1Refs.ToMultiByteArray());
            var map = new Dictionary<string, bool>();
            for (int i = 0; i < sha1Refs.Length; i++)
            {
                var sha1Ref = sha1Refs[i];
                map[sha1Ref] = intFlags[i].FromUtf8Bytes() == "1";
            }
            return map;
        }

        public void RemoveAllLuaScripts()
        {
            base.ScriptFlush();
        }

        public void KillRunningLuaScript()
        {
            base.ScriptKill();
        }

        public string LoadLuaScript(string body)
        {
            return base.ScriptLoad(body).FromUtf8Bytes();
        }

        #endregion

        public void RemoveByPattern(string pattern)
        {
            List<string> keys = Keys(pattern).ToStringList();
            if (keys.Count > 0)
                Del(keys.ToArray());
        }

        public void RemoveByRegex(string pattern)
        {
            RemoveByPattern(pattern.Replace(".*", "*").Replace(".+", "?"));
        }
    }

}