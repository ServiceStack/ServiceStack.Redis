//
// redis-sharp.cs: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Miguel de Icaza (miguel@gnome.org)
//
// Copyright 2010 Novell, Inc.
//
// Licensed under the same terms of Redis: new BSD license.
//
//#define DEBUG

using System;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using ServiceStack.Logging;
using ServiceStack.Redis;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    /// <summary>
    /// This class contains all the common operations for the RedisClient.
    /// The client contains a 1:1 mapping of c# methods to redis operations of the same name.
    /// 
    /// Not threadsafe use a pooled manager
    /// </summary>
    public partial class RedisNativeClient
        : IRedisNativeClient
    {
        private static readonly ILog log = LogManager.GetLogger(typeof(RedisNativeClient));

        public const int DefaultDb = 0;
        public const int DefaultPort = 6379;
        public const string DefaultHost = "localhost";

        internal const int Success = 1;
        internal const int OneGb = 1073741824;
        private readonly byte[] endData = new[] { (byte)'\r', (byte)'\n' };

        private int clientPort;
        private string lastCommand;
        private SocketException lastSocketException;
        public bool HadExceptions { get; protected set; }

        protected Socket socket;
        protected BufferedStream Bstream;

        private IRedisTransactionBase transaction;
        private IRedisPipelineShared pipeline;

        private Dictionary<string, string> info;
        /// <summary>
        /// Used to manage connection pooling
        /// </summary>
        internal bool Active { get; set; }
        internal PooledRedisClientManager ClientManager { get; set; }

        internal int IdleTimeOutSecs = 240; //default on redis is 300
        internal long LastConnectedAtTimestamp;

        public int Id { get; set; }

        public string Host { get; private set; }
        public int Port { get; private set; }

        /// <summary>
        /// Gets or sets object key prefix.
        /// </summary>
        public string NamespacePrefix { get; set; }
        public int ConnectTimeout { get; set; }
        public int RetryTimeout { get; set; }
        public int RetryCount { get; set; }
        public int SendTimeout { get; set; }
		public int ReceiveTimeout { get; set; }
        public string Password { get; set; }

        public Action<IRedisNativeClient> ConnectionFilter { get; set; }

        internal IRedisTransactionBase Transaction
        {
            get
            {
                return transaction;
            }
            set
            {
                if (value != null)
                    AssertConnectedSocket();
                transaction = value;
            }
        }


        internal IRedisPipelineShared Pipeline
        {
            get
            {
                return pipeline;
            }
            set
            {
                if (value != null)
                    AssertConnectedSocket();
                pipeline = value;
            }
        }

        public RedisNativeClient(string host)
            : this(host, DefaultPort) {}

        public RedisNativeClient(string host, int port)
            : this(host, port, null) {}

        public RedisNativeClient(string host, int port, string password = null, int db = DefaultDb)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            Host = host;
            Port = port;
            SendTimeout = -1;
			ReceiveTimeout = -1;
            Password = password;
            Db = db;
        }

        public RedisNativeClient()
            : this(DefaultHost, DefaultPort) {}
        
        #region Common Operations

        int db;
        public int Db
        {
            get
            {
                return db;
            }

            set
            {
                db = value;
            }
        }

        public int DbSize
        {
            get
            {
                return SendExpectInt(Commands.DbSize);
            }
        }

        public DateTime LastSave
        {
            get
            {
                var t = SendExpectInt(Commands.LastSave);
                return DateTimeExtensions.FromUnixTime(t);
            }
        }

    	public Dictionary<string, string> Info
        {
            get
            {
                if (this.info == null)
                {
                    var lines = SendExpectString(Commands.Info);
                    this.info = new Dictionary<string, string>();

                    foreach (var line in lines
                        .Split(new[] { "\r\n" }, StringSplitOptions.RemoveEmptyEntries))
                    {
                        var p = line.IndexOf(':');
                        if (p == -1) continue;

                        this.info.Add(line.Substring(0, p), line.Substring(p + 1));
                    }
                }
                return this.info;
            }
        }

        public string ServerVersion
        {
            get
            {
                string version;
                this.Info.TryGetValue("redis_version", out version);
                return version;
            }
        }

        public bool Ping()
        {
            return SendExpectCode(Commands.Ping) == "PONG";
        }

        public string Echo(string text)
        {
            return SendExpectData(Commands.Echo, text.ToUtf8Bytes()).FromUtf8Bytes();
        }

        public void SlaveOf(string hostname, int port)
        {
            SendExpectSuccess(Commands.SlaveOf, hostname.ToUtf8Bytes(), port.ToUtf8Bytes());
        }

        public void SlaveOfNoOne()
        {
            SendExpectSuccess(Commands.SlaveOf, Commands.No, Commands.One);
        }

    	public byte[][] ConfigGet(string pattern)
    	{
			return SendExpectMultiData(Commands.Config, Commands.Get, pattern.ToUtf8Bytes());
		}

    	public void ConfigSet(string item, byte[] value)
    	{
			SendExpectSuccess(Commands.Config, Commands.Set, item.ToUtf8Bytes(), value);
		}

    	public void ConfigResetStat()
    	{
			SendExpectSuccess(Commands.Config, Commands.ResetStat);
		}

    	public byte[][] Time()
    	{
			return SendExpectMultiData(Commands.Time);
		}

    	public void DebugSegfault()
    	{
			SendExpectSuccess(Commands.Debug, Commands.Segfault);
		}

    	public byte[] Dump(string key)
		{
			if (key == null)
				throw new ArgumentNullException("key");
			
			return SendExpectData(Commands.Dump);
		}

    	public byte[] Restore(string key, long expireMs, byte[] dumpValue)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectData(Commands.Restore, key.ToUtf8Bytes(), expireMs.ToUtf8Bytes(), dumpValue);
		}

    	public void Migrate(string host, int port, int destinationDb, long timeoutMs)
    	{
			SendExpectSuccess(Commands.Migrate, host.ToUtf8Bytes(), port.ToUtf8Bytes(), destinationDb.ToUtf8Bytes(), timeoutMs.ToUtf8Bytes());
		}

    	public bool Move(string key, int db)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.Move, key.ToUtf8Bytes(), db.ToUtf8Bytes()) == Success;
		}

    	public int ObjectIdleTime(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");
			
			return SendExpectInt(Commands.Object, Commands.IdleTime, key.ToUtf8Bytes());
		}

    	public string Type(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectCode(Commands.Type, key.ToUtf8Bytes());
        }

        public RedisKeyType GetEntryType(string key)
        {
            switch (Type(key))
            {
                case "none":
                    return RedisKeyType.None;
                case "string":
                    return RedisKeyType.String;
                case "set":
                    return RedisKeyType.Set;
                case "list":
                    return RedisKeyType.List;
                case "zset":
                    return RedisKeyType.SortedSet;
                case "hash":
                    return RedisKeyType.Hash;
            }
            throw CreateResponseError("Invalid value");
        }

    	public int StrLen(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.StrLen, key.ToUtf8Bytes());
		}

    	public void Set(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            SendExpectSuccess(Commands.Set, key.ToUtf8Bytes(), value);
        }

        public void SetEx(string key, int expireInSeconds, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            SendExpectSuccess(Commands.SetEx, key.ToUtf8Bytes(), expireInSeconds.ToUtf8Bytes(), value);
        }

    	public bool Persist(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.Persist, key.ToUtf8Bytes()) == Success;
		}

    	public void PSetEx(string key, long expireInMs, byte[] value)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			SendExpectSuccess(Commands.PSetEx, expireInMs.ToUtf8Bytes(), key.ToUtf8Bytes(), value);
		}

    	public int SetNX(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            return SendExpectInt(Commands.SetNx, key.ToUtf8Bytes(), value);
        }

    	public void MSet(byte[][] keys, byte[][] values)
    	{
    		var keysAndValues = MergeCommandWithKeysAndValues(Commands.MSet, keys, values);

    		SendExpectSuccess(keysAndValues);
    	}

    	public void MSet(string[] keys, byte[][] values)
    	{
			MSet(keys.ToMultiByteArray(), values);
		}

    	public bool MSetNx(byte[][] keys, byte[][] values)
    	{
			var keysAndValues = MergeCommandWithKeysAndValues(Commands.MSet, keys, values);

			return SendExpectInt(keysAndValues) == Success;
		}

    	public bool MSetNx(string[] keys, byte[][] values)
    	{
			return MSetNx(keys.ToMultiByteArray(), values);
		}

    	public byte[] Get(string key)
        {
            return GetBytes(key);
        }

        public object[] Slowlog(int ? top)
        {
            if (top.HasValue)
                return SendExpectDeeplyNestedMultiData(Commands.Slowlog, Commands.Get, top.Value.ToUtf8Bytes());
            else
                return SendExpectDeeplyNestedMultiData(Commands.Slowlog, Commands.Get);
        }

        public void SlowlogReset()
        {
            SendExpectSuccess(Commands.Slowlog, "RESET".ToUtf8Bytes());
        }

        public byte[] GetBytes(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectData(Commands.Get, key.ToUtf8Bytes());
        }

        public byte[] GetSet(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            return SendExpectData(Commands.GetSet, key.ToUtf8Bytes(), value);
        }

        public int Exists(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.Exists, key.ToUtf8Bytes());
        }

        public int Del(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.Del, key.ToUtf8Bytes());
        }

        public int Del(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.Del, keys);
            return SendExpectInt(cmdWithArgs);
        }

        public long Incr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Incr, key.ToUtf8Bytes());
        }

        public long IncrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.IncrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

    	public double IncrByFloat(string key, double incrBy)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectDouble(Commands.IncrBy, key.ToUtf8Bytes(), incrBy.ToUtf8Bytes());
		}

    	public long Decr(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Decr, key.ToUtf8Bytes());
        }

        public long DecrBy(string key, int count)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.DecrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

        public int Append(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.Append, key.ToUtf8Bytes(), value);
        }
        
        public byte[] Substr(string key, int fromIndex, int toIndex)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectData(Commands.Substr, key.ToUtf8Bytes(), fromIndex.ToUtf8Bytes(), toIndex.ToUtf8Bytes());
        }

    	public byte[] GetRange(string key, int fromIndex, int toIndex)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectData(Commands.GetRange, key.ToUtf8Bytes(), fromIndex.ToUtf8Bytes(), toIndex.ToUtf8Bytes());
		}

    	public int SetRange(string key, int offset, byte[] value)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.SetRange, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value);
		}

    	public int GetBit(string key, int offset)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.GetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes());
        }

        public int SetBit(string key, int offset, int value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value > 1 || value < 0)
                throw  new ArgumentException("value is out of range");

            return SendExpectInt(Commands.SetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value.ToUtf8Bytes());
        }

        public string RandomKey()
        {
            return SendExpectData(Commands.RandomKey).FromUtf8Bytes();
        }

        public void Rename(string oldKeyname, string newKeyname)
        {
            if (oldKeyname == null)
                throw new ArgumentNullException("oldKeyname");
            if (newKeyname == null)
                throw new ArgumentNullException("newKeyname");

            SendExpectSuccess(Commands.Rename, oldKeyname.ToUtf8Bytes(), newKeyname.ToUtf8Bytes());
        }

    	public bool RenameNx(string oldKeyname, string newKeyname)
    	{
			if (oldKeyname == null)
				throw new ArgumentNullException("oldKeyname");
			if (newKeyname == null)
				throw new ArgumentNullException("newKeyname");

			return SendExpectInt(Commands.RenameNx, oldKeyname.ToUtf8Bytes(), newKeyname.ToUtf8Bytes()) == Success;
		}

    	public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

			return SendExpectInt(Commands.Expire, key.ToUtf8Bytes(), seconds.ToUtf8Bytes()) == Success;
        }

		public bool PExpire(string key, long ttlMs)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.PExpire, key.ToUtf8Bytes(), ttlMs.ToUtf8Bytes()) == Success;
		}

    	public bool ExpireAt(string key, long unixTime)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.ExpireAt, key.ToUtf8Bytes(), unixTime.ToUtf8Bytes()) == Success;
        }

    	public bool PExpireAt(string key, long unixTimeMs)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectInt(Commands.PExpireAt, key.ToUtf8Bytes(), unixTimeMs.ToUtf8Bytes()) == Success;
		}

    	public int Ttl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectInt(Commands.Ttl, key.ToUtf8Bytes());
        }

    	public long PTtl(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.PTtl, key.ToUtf8Bytes());
		}

    	public void Save()
        {
            SendExpectSuccess(Commands.Save);
        }

        public void SaveAsync()
        {
            BgSave();
        }

        public void BgSave()
        {
            SendExpectSuccess(Commands.BgSave);
        }

        public void Shutdown()
        {
            SendCommand(Commands.Shutdown);
        }

        public void BgRewriteAof()
        {
            SendExpectSuccess(Commands.BgRewriteAof);
        }

        public void Quit()
        {
            SendCommand(Commands.Quit);
        }

        public void FlushDb()
        {
            SendExpectSuccess(Commands.FlushDb);
        }

        public void FlushAll()
        {
            SendExpectSuccess(Commands.FlushAll);
        }

        public byte[][] Keys(string pattern)
        {
            if (pattern == null)
                throw new ArgumentNullException("pattern");

            return SendExpectMultiData(Commands.Keys, pattern.ToUtf8Bytes());
        }

        public byte[][] MGet(params byte[][] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (keys.Length == 0)
                throw new ArgumentException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.MGet, keys);

            return SendExpectMultiData(cmdWithArgs);
        }

        public byte[][] MGet(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (keys.Length == 0)
                throw new ArgumentException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.MGet, keys);

            return SendExpectMultiData(cmdWithArgs);
        }

        public void Watch(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (keys.Length == 0)
                throw new ArgumentException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.Watch, keys);

            SendExpectCode(cmdWithArgs);

        }
        public void UnWatch()
        {
            SendExpectCode(Commands.UnWatch);
        }
        internal void Multi()
        {
            //make sure socket is connected. Otherwise, fetch of server info will interfere
            //with pipeline
            AssertConnectedSocket();
            if (!SendCommand(Commands.Multi))
                throw CreateConnectionError();
        }

        /// <summary>
        /// Requires custom result parsing
        /// </summary>
        /// <returns>Number of results</returns>
        internal void Exec()
        {
            if (!SendCommand(Commands.Exec))
                throw CreateConnectionError();

        }

        internal void Discard()
        {
            SendExpectSuccess(Commands.Discard);
        }

        #endregion


        #region Set Operations

        public byte[][] SMembers(string setId)
        {
            return SendExpectMultiData(Commands.SMembers, setId.ToUtf8Bytes());
        }

        public int SAdd(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectInt(Commands.SAdd, setId.ToUtf8Bytes(), value);
        }

        public int SRem(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectInt(Commands.SRem, setId.ToUtf8Bytes(), value);
        }

        public byte[] SPop(string setId)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectData(Commands.SPop, setId.ToUtf8Bytes());
        }

        public void SMove(string fromSetId, string toSetId, byte[] value)
        {
            if (fromSetId == null)
                throw new ArgumentNullException("fromSetId");
            if (toSetId == null)
                throw new ArgumentNullException("toSetId");

            SendExpectSuccess(Commands.SMove, fromSetId.ToUtf8Bytes(), toSetId.ToUtf8Bytes(), value);
        }

        public int SCard(string setId)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.SCard, setId.ToUtf8Bytes());
        }

        public int SIsMember(string setId, byte[] value)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.SIsMember, setId.ToUtf8Bytes(), value);
        }

        public byte[][] SInter(params string[] setIds)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.SInter, setIds);
            return SendExpectMultiData(cmdWithArgs);
        }

        public void SInterStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SInterStore, setIdsList.ToArray());
            SendExpectSuccess(cmdWithArgs);
        }

        public byte[][] SUnion(params string[] setIds)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.SUnion, setIds);
            return SendExpectMultiData(cmdWithArgs);
        }

        public void SUnionStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SUnionStore, setIdsList.ToArray());
            SendExpectSuccess(cmdWithArgs);
        }

        public byte[][] SDiff(string fromSetId, params string[] withSetIds)
        {
            var setIdsList = new List<string>(withSetIds);
            setIdsList.Insert(0, fromSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SDiff, setIdsList.ToArray());
            return SendExpectMultiData(cmdWithArgs);
        }

        public void SDiffStore(string intoSetId, string fromSetId, params string[] withSetIds)
        {
            var setIdsList = new List<string>(withSetIds);
            setIdsList.Insert(0, fromSetId);
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.SDiffStore, setIdsList.ToArray());
            SendExpectSuccess(cmdWithArgs);
        }

        public byte[] SRandMember(string setId)
        {
            return SendExpectData(Commands.SRandMember, setId.ToUtf8Bytes());
        }

        #endregion


        #region List Operations

        public byte[][] LRange(string listId, int startingFrom, int endingAt)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectMultiData(Commands.LRange, listId.ToUtf8Bytes(), startingFrom.ToUtf8Bytes(), endingAt.ToUtf8Bytes());
        }

        public byte[][] Sort(string listOrSetId, SortOptions sortOptions)
        {
            var cmdWithArgs = new List<byte[]>
           	{
           		Commands.Sort, listOrSetId.ToUtf8Bytes()
           	};

            if (sortOptions.SortPattern != null)
            {
                cmdWithArgs.Add(Commands.By);
                cmdWithArgs.Add(sortOptions.SortPattern.ToUtf8Bytes());
            }

            if (sortOptions.Skip.HasValue || sortOptions.Take.HasValue)
            {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(sortOptions.Skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(sortOptions.Take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            if (sortOptions.GetPattern != null)
            {
                cmdWithArgs.Add(Commands.Get);
                cmdWithArgs.Add(sortOptions.GetPattern.ToUtf8Bytes());
            }

            if (sortOptions.SortDesc)
            {
                cmdWithArgs.Add(Commands.Desc);
            }

            if (sortOptions.SortAlpha)
            {
                cmdWithArgs.Add(Commands.Alpha);
            }

            if (sortOptions.StoreAtKey != null)
            {
                cmdWithArgs.Add(Commands.Store);
                cmdWithArgs.Add(sortOptions.StoreAtKey.ToUtf8Bytes());
            }

            return SendExpectMultiData(cmdWithArgs.ToArray());
        }

        public int RPush(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectInt(Commands.RPush, listId.ToUtf8Bytes(), value);
        }

    	public int RPushX(string listId, byte[] value)
    	{
    		throw new NotImplementedException();
    	}

    	public int LPush(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectInt(Commands.LPush, listId.ToUtf8Bytes(), value);
        }

    	public int LPushX(string listId, byte[] value)
    	{
    		throw new NotImplementedException();
    	}

    	public void LTrim(string listId, int keepStartingFrom, int keepEndingAt)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            SendExpectSuccess(Commands.LTrim, listId.ToUtf8Bytes(), keepStartingFrom.ToUtf8Bytes(), keepEndingAt.ToUtf8Bytes());
        }

        public int LRem(string listId, int removeNoOfMatches, byte[] value)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectInt(Commands.LRem, listId.ToUtf8Bytes(), removeNoOfMatches.ToUtf8Bytes(), value);
        }

        public int LLen(string listId)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectInt(Commands.LLen, listId.ToUtf8Bytes());
        }

        public byte[] LIndex(string listId, int listIndex)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectData(Commands.LIndex, listId.ToUtf8Bytes(), listIndex.ToUtf8Bytes());
        }

        public void LInsert(string listId, bool insertBefore, byte[] pivot, byte[] value)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            var position = insertBefore ? Commands.Before : Commands.After;

            SendExpectSuccess(Commands.LInsert, listId.ToUtf8Bytes(), position, pivot, value);
        }

        public void LSet(string listId, int listIndex, byte[] value)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            SendExpectSuccess(Commands.LSet, listId.ToUtf8Bytes(), listIndex.ToUtf8Bytes(), value);
        }

        public byte[] LPop(string listId)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectData(Commands.LPop, listId.ToUtf8Bytes());
        }

        public byte[] RPop(string listId)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectData(Commands.RPop, listId.ToUtf8Bytes());
        }

        public byte[][] BLPop(string listId, int timeOutSecs)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectMultiData(Commands.BLPop, listId.ToUtf8Bytes(), timeOutSecs.ToUtf8Bytes());
        }

        public byte[][] BLPop(string[] listIds, int timeOutSecs)
        {
            if (listIds == null)
                throw new ArgumentNullException("listIds");
            var args = new List<byte[]>();
            args.Add(Commands.BLPop);
            args.AddRange(listIds.Select(listId => listId.ToUtf8Bytes()));
            args.Add(timeOutSecs.ToUtf8Bytes());            
            return SendExpectMultiData(args.ToArray());
        }

        public byte[] BLPopValue(string listId, int timeOutSecs)
        {
            var blockingResponse = BLPop(new[]{listId}, timeOutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse[1];
        }

        public byte[][] BLPopValue(string[] listIds, int timeOutSecs)
        {
            var blockingResponse = BLPop(listIds, timeOutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse;
        }

        public byte[][] BRPop(string listId, int timeOutSecs)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectMultiData(Commands.BRPop, listId.ToUtf8Bytes(), timeOutSecs.ToUtf8Bytes());
        }

        public byte[][] BRPop(string[] listIds, int timeOutSecs)
        {
            if (listIds == null)
                throw new ArgumentNullException("listIds");
            var args = new List<byte[]>();
            args.Add(Commands.BRPop);
            args.AddRange(listIds.Select(listId => listId.ToUtf8Bytes()));
            args.Add(timeOutSecs.ToUtf8Bytes());            
            return SendExpectMultiData(args.ToArray());
        }

        public byte[] BRPopValue(string listId, int timeOutSecs)
        {
            var blockingResponse = BRPop(new[]{listId}, timeOutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse[1];
        }

        public byte[][] BRPopValue(string[] listIds, int timeOutSecs)
        {
            var blockingResponse = BRPop(listIds, timeOutSecs);
            return blockingResponse.Length == 0
                ? null
                : blockingResponse;
        }

        public byte[] RPopLPush(string fromListId, string toListId)
        {
            if (fromListId == null)
                throw new ArgumentNullException("fromListId");
            if (toListId == null)
                throw new ArgumentNullException("toListId");

            return SendExpectData(Commands.RPopLPush, fromListId.ToUtf8Bytes(), toListId.ToUtf8Bytes());
        }

        public byte[] BRPopLPush(string fromListId, string toListId, int timeOutSecs)
        {
            if (fromListId == null)
                throw new ArgumentNullException("fromListId");
            if (toListId == null)
                throw new ArgumentNullException("toListId");

            byte[][] result= SendExpectMultiData(Commands.BRPopLPush, fromListId.ToUtf8Bytes(), toListId.ToUtf8Bytes(), timeOutSecs.ToUtf8Bytes());
            return result.Length == 0 ? null : result[1];
        }

        #endregion


        #region Sorted Set Operations

        private static void AssertSetIdAndValue(string setId, byte[] value)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (value == null)
                throw new ArgumentNullException("value");
        }

		public int ZAdd(string setId, double score, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectInt(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToFastUtf8Bytes(), value);
		}

		public int ZAdd(string setId, long score, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectInt(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToUtf8Bytes(), value);
		}

        public int ZRem(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectInt(Commands.ZRem, setId.ToUtf8Bytes(), value);
        }

		public double ZIncrBy(string setId, double incrBy, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectDouble(Commands.ZIncrBy, setId.ToUtf8Bytes(), incrBy.ToFastUtf8Bytes(), value);
		}

		public double ZIncrBy(string setId, long incrBy, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectDouble(Commands.ZIncrBy, setId.ToUtf8Bytes(), incrBy.ToUtf8Bytes(), value);
		}

        public int ZRank(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectInt(Commands.ZRank, setId.ToUtf8Bytes(), value);
        }

        public int ZRevRank(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectInt(Commands.ZRevRank, setId.ToUtf8Bytes(), value);
        }

        private byte[][] GetRange(byte[] commandBytes, string setId, int min, int max, bool withScores)
        {
            if (string.IsNullOrEmpty(setId))
                throw new ArgumentNullException("setId");

            var cmdWithArgs = new List<byte[]>
           	{
           		commandBytes, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes()
           	};

            if (withScores)
            {
                cmdWithArgs.Add(Commands.WithScores);
            }

            return SendExpectMultiData(cmdWithArgs.ToArray());
        }

        public byte[][] ZRange(string setId, int min, int max)
        {
            return SendExpectMultiData(Commands.ZRange, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public byte[][] ZRangeWithScores(string setId, int min, int max)
        {
            return GetRange(Commands.ZRange, setId, min, max, true);
        }

        public byte[][] ZRevRange(string setId, int min, int max)
        {
            return GetRange(Commands.ZRevRange, setId, min, max, false);
        }

        public byte[][] ZRevRangeWithScores(string setId, int min, int max)
        {
            return GetRange(Commands.ZRevRange, setId, min, max, true);
        }

        private byte[][] GetRangeByScore(byte[] commandBytes,
            string setId, double min, double max, int? skip, int? take, bool withScores)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            var cmdWithArgs = new List<byte[]>
           	{
           		commandBytes, setId.ToUtf8Bytes(), min.ToFastUtf8Bytes(), max.ToFastUtf8Bytes()
           	};

            if (skip.HasValue || take.HasValue)
            {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            if (withScores)
            {
                cmdWithArgs.Add(Commands.WithScores);
            }

            return SendExpectMultiData(cmdWithArgs.ToArray());
        }

		private byte[][] GetRangeByScore(byte[] commandBytes,
			string setId, long min, long max, int? skip, int? take, bool withScores)
		{
			if (setId == null)
				throw new ArgumentNullException("setId");

			var cmdWithArgs = new List<byte[]>
           	{
           		commandBytes, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes()
           	};

			if (skip.HasValue || take.HasValue)
			{
				cmdWithArgs.Add(Commands.Limit);
				cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
				cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
			}

			if (withScores)
			{
				cmdWithArgs.Add(Commands.WithScores);
			}

			return SendExpectMultiData(cmdWithArgs.ToArray());
		}

		public byte[][] ZRangeByScore(string setId, double min, double max, int? skip, int? take)
		{
			return GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, false);
		}

		public byte[][] ZRangeByScore(string setId, long min, long max, int? skip, int? take)
		{
			return GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, false);
		}

		public byte[][] ZRangeByScoreWithScores(string setId, double min, double max, int? skip, int? take)
		{
			return GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, true);
		}

		public byte[][] ZRangeByScoreWithScores(string setId, long min, long max, int? skip, int? take)
		{
			return GetRangeByScore(Commands.ZRangeByScore, setId, min, max, skip, take, true);
		}

		public byte[][] ZRevRangeByScore(string setId, double min, double max, int? skip, int? take)
		{
			//Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
			return GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, false);
		}

		public byte[][] ZRevRangeByScore(string setId, long min, long max, int? skip, int? take)
		{
			//Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
			return GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, false);
		}

		public byte[][] ZRevRangeByScoreWithScores(string setId, double min, double max, int? skip, int? take)
		{
			//Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
			return GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, true);
		}

		public byte[][] ZRevRangeByScoreWithScores(string setId, long min, long max, int? skip, int? take)
		{
			//Note: http://redis.io/commands/zrevrangebyscore has max, min in the wrong other
			return GetRangeByScore(Commands.ZRevRangeByScore, setId, max, min, skip, take, true);
		}

        public int ZRemRangeByRank(string setId, int min, int max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.ZRemRangeByRank, setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

		public int ZRemRangeByScore(string setId, double fromScore, double toScore)
		{
			if (setId == null)
				throw new ArgumentNullException("setId");

			return SendExpectInt(Commands.ZRemRangeByScore, setId.ToUtf8Bytes(),
				fromScore.ToFastUtf8Bytes(), toScore.ToFastUtf8Bytes());
		}

		public int ZRemRangeByScore(string setId, long fromScore, long toScore)
		{
			if (setId == null)
				throw new ArgumentNullException("setId");

			return SendExpectInt(Commands.ZRemRangeByScore, setId.ToUtf8Bytes(),
				fromScore.ToUtf8Bytes(), toScore.ToUtf8Bytes());
		}

        public int ZCard(string setId)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.ZCard, setId.ToUtf8Bytes());
        }

        public int ZCount(string setId, double min, double max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public int ZCount(string setId, long min, long max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectInt(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public double ZScore(string setId, byte[] value)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectDouble(Commands.ZScore, setId.ToUtf8Bytes(), value);
        }

        public int ZUnionStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZUnionStore, setIdsList.ToArray());
            return SendExpectInt(cmdWithArgs);
        }

        public int ZInterStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZInterStore, setIdsList.ToArray());
            return SendExpectInt(cmdWithArgs);
        }

        #endregion


        #region Hash Operations

        private static void AssertHashIdAndKey(string hashId, byte[] key)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");
            if (key == null)
                throw new ArgumentNullException("key");
        }

        public int HSet(string hashId, byte[] key, byte[] value)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HSet, hashId.ToUtf8Bytes(), key, value);
        }

        public int HSetNX(string hashId, byte[] key, byte[] value)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HSetNx, hashId.ToUtf8Bytes(), key, value);
        }

        public void HMSet(string hashId, byte[][] keys, byte[][] values)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");

            var cmdArgs = MergeCommandWithKeysAndValues(Commands.HMSet, hashId.ToUtf8Bytes(), keys, values);

            SendExpectSuccess(cmdArgs);
        }

        public int HIncrby(string hashId, byte[] key, int incrementBy)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

        public int HIncrby(string hashId, byte[] key, long incrementBy)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

    	public double HIncrbyFloat(string hashId, byte[] key, double incrementBy)
    	{
			AssertHashIdAndKey(hashId, key);

			return SendExpectDouble(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString(CultureInfo.InvariantCulture).ToUtf8Bytes());
		}

    	public byte[] HGet(string hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectData(Commands.HGet, hashId.ToUtf8Bytes(), key);
        }

        public byte[][] HMGet(string hashId, params byte[][] keys)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");
            if (keys.Length == 0)
                throw new ArgumentNullException("keys");

            var cmdArgs = MergeCommandWithArgs(Commands.HMGet, hashId.ToUtf8Bytes(), keys);

            return SendExpectMultiData(cmdArgs);
        }

        public int HDel(string hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HDel, hashId.ToUtf8Bytes(), key);
        }

        public int HExists(string hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectInt(Commands.HExists, hashId.ToUtf8Bytes(), key);
        }

        public int HLen(string hashId)
        {
            if (string.IsNullOrEmpty(hashId))
                throw new ArgumentNullException("hashId");

            return SendExpectInt(Commands.HLen, hashId.ToUtf8Bytes());
        }

        public byte[][] HKeys(string hashId)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");

            return SendExpectMultiData(Commands.HKeys, hashId.ToUtf8Bytes());
        }

        public byte[][] HVals(string hashId)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");

            return SendExpectMultiData(Commands.HVals, hashId.ToUtf8Bytes());
        }

        public byte[][] HGetAll(string hashId)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");

            return SendExpectMultiData(Commands.HGetAll, hashId.ToUtf8Bytes());
        }

        public int Publish(string toChannel, byte[] message)
        {
            return SendExpectInt(Commands.Publish, toChannel.ToUtf8Bytes(), message);
        }

        public byte[][] ReceiveMessages()
        {
            return ReadMultiData();
        }

    	public byte[][] Subscribe(params string[] toChannels)
        {
            if (toChannels.Length == 0)
                throw new ArgumentNullException("toChannels");

            var cmdWithArgs = MergeCommandWithArgs(Commands.Subscribe, toChannels);
            return SendExpectMultiData(cmdWithArgs);
        }

        public byte[][] UnSubscribe(params string[] fromChannels)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.UnSubscribe, fromChannels);
            return SendExpectMultiData(cmdWithArgs);
        }

        public byte[][] PSubscribe(params string[] toChannelsMatchingPatterns)
        {
            if (toChannelsMatchingPatterns.Length == 0)
                throw new ArgumentNullException("toChannelsMatchingPatterns");

            var cmdWithArgs = MergeCommandWithArgs(Commands.PSubscribe, toChannelsMatchingPatterns);
            return SendExpectMultiData(cmdWithArgs);
        }

        public byte[][] PUnSubscribe(params string[] fromChannelsMatchingPatterns)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PUnSubscribe, fromChannelsMatchingPatterns);
            return SendExpectMultiData(cmdWithArgs);
        }

        public RedisPipelineCommand CreatePipelineCommand()
        {
            AssertConnectedSocket();
            return new RedisPipelineCommand(this);
        }

        #endregion

        internal bool IsDisposed { get; set; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~RedisNativeClient()
        {
            Dispose(false);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (ClientManager != null)
            {
                ClientManager.DisposeClient(this);
                return;
            }

            if (disposing)
            {
                //dispose un managed resources
                DisposeConnection();
            }
        }

        internal void DisposeConnection()
        {
			if (IsDisposed) return;
            IsDisposed = true;

            if (socket == null) return;

            try
            {
                Quit();
            }
            catch (Exception ex)
            {
                log.Error("Error when trying to Quit()", ex);
            }
            finally
            {
                SafeConnectionClose();
            }
        }

        private void SafeConnectionClose()
        {
            try
            {
                // workaround for a .net bug: http://support.microsoft.com/kb/821625
                if (Bstream != null)
                    Bstream.Close();
            }
            catch { }
            try
            {
                if (socket != null)
                    socket.Close();
            }
            catch { }
            Bstream = null;
            socket = null;
        }
    }
}
