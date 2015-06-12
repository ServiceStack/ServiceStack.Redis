﻿//
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
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using ServiceStack.Logging;
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

        public const long DefaultDb = 0;
        public const int DefaultPort = 6379;
        public const int DefaultPortSsl = 6380;
        public const int DefaultPortSentinel = 6379;
        public const string DefaultHost = "localhost";
        public const int DefaultIdleTimeOutSecs = 240; //default on redis is 300

        internal const int Success = 1;
        internal const int OneGb = 1073741824;
        private readonly byte[] endData = new[] { (byte)'\r', (byte)'\n' };

        private int clientPort;
        private string lastCommand;
        private SocketException lastSocketException;
        public bool HadExceptions { get; protected set; }

        protected Socket socket;
        protected BufferedStream Bstream;
        protected SslStream sslStream;

        private IRedisTransactionBase transaction;
        private IRedisPipelineShared pipeline;

        private Dictionary<string, string> info;

        const int YES = 1;
        const int NO = 0;

        /// <summary>
        /// Used to manage connection pooling
        /// </summary>
        private int active;
        internal bool Active
        {
            get { return Interlocked.CompareExchange(ref active, 0, 0) == YES; }
            set
            {
                var intValue = value ? YES : NO;
                Interlocked.CompareExchange(ref active, intValue, active);
            }
        }

        internal IHandleClientDispose ClientManager { get; set; }

        internal long LastConnectedAtTimestamp;

        public long Id { get; set; }

        public string Host { get; private set; }
        public int Port { get; private set; }
        public bool Ssl { get; private set; }
        
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
        public string Client { get; set; }
        public int IdleTimeOutSecs { get; set; }

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

        public RedisNativeClient(string connectionString)
            : this(connectionString.ToRedisEndpoint()) {}

        public RedisNativeClient(RedisEndpoint config)
        {
            Init(config);
        }

        public RedisNativeClient(string host, int port)
            : this(host, port, null) {}

        public RedisNativeClient(string host, int port, string password = null, long db = DefaultDb)
        {
            if (host == null)
                throw new ArgumentNullException("host");

            Init(new RedisEndpoint(host, port, password, db));
        }

        private void Init(RedisEndpoint config)
        {
            Host = config.Host;
            Port = config.Port;
            ConnectTimeout = config.ConnectTimeout;
            SendTimeout = config.SendTimeout;
            ReceiveTimeout = config.ReceiveTimeout;
            Password = config.Password;
            NamespacePrefix = config.NamespacePrefix;
            Client = config.Client;
            Db = config.Db;
            Ssl = config.Ssl;
            IdleTimeOutSecs = config.IdleTimeOutSecs;
        }

        public RedisNativeClient()
            : this(DefaultHost, DefaultPort) {}
        
        #region Common Operations

        long db;
        public long Db
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

        public long DbSize
        {
            get
            {
                return SendExpectLong(Commands.DbSize);
            }
        }

        public DateTime LastSave
        {
            get
            {
                var t = SendExpectLong(Commands.LastSave);
                return t.FromUnixTime();
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

        public RedisData RawCommand(params object[] cmdWithArgs)
        {
            var byteArgs = new List<byte[]>();

            foreach (var arg in cmdWithArgs)
            {
                if (arg == null)
                {
                    byteArgs.Add(new byte[0]);
                    continue;
                }

                var bytes = arg as byte[];
                if (bytes != null)
                {
                    byteArgs.Add(bytes);
                }
                else if (arg.GetType().IsUserType())
                {
                    var json = arg.ToJson();
                    byteArgs.Add(json.ToUtf8Bytes());
                }
                else 
                {
                    var str = arg.ToString();
                    byteArgs.Add(str.ToUtf8Bytes());
                }
            }

            var data = SendExpectComplexResponse(byteArgs.ToArray());
            return data;
        }

        public RedisData RawCommand(params byte[][] cmdWithBinaryArgs)
        {
            return SendExpectComplexResponse(cmdWithBinaryArgs);
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

        public void ConfigRewrite()
        {
            SendExpectSuccess(Commands.Config, Commands.Rewrite);
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
			
			return SendExpectData(Commands.Dump, key.ToUtf8Bytes());
		}

    	public byte[] Restore(string key, long expireMs, byte[] dumpValue)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectData(Commands.Restore, key.ToUtf8Bytes(), expireMs.ToUtf8Bytes(), dumpValue);
		}

    	public void Migrate(string host, int port, string key, int destinationDb, long timeoutMs)
    	{
            if (key == null)
                throw new ArgumentNullException("key");

			SendExpectSuccess(Commands.Migrate, host.ToUtf8Bytes(), port.ToUtf8Bytes(), key.ToUtf8Bytes(), destinationDb.ToUtf8Bytes(), timeoutMs.ToUtf8Bytes());
		}

    	public bool Move(string key, int db)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.Move, key.ToUtf8Bytes(), db.ToUtf8Bytes()) == Success;
		}

    	public long ObjectIdleTime(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");
			
			return SendExpectLong(Commands.Object, Commands.IdleTime, key.ToUtf8Bytes());
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

    	public long StrLen(string key)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.StrLen, key.ToUtf8Bytes());
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

    	public void Set(string key, byte[] value, int expirySeconds, long expiryMs = 0)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            if (expirySeconds > 0)
                SendExpectSuccess(Commands.Set, key.ToUtf8Bytes(), value, Commands.Ex, expirySeconds.ToUtf8Bytes());
            else if (expiryMs > 0)
                SendExpectSuccess(Commands.Set, key.ToUtf8Bytes(), value, Commands.Px, expiryMs.ToUtf8Bytes());
            else
                SendExpectSuccess(Commands.Set, key.ToUtf8Bytes(), value);
        }

        public bool Set(string key, byte[] value, bool exists, int expirySeconds = 0, long expiryMs = 0)
        {
            var entryExists = exists ? Commands.Xx : Commands.Nx;

            if (expirySeconds > 0)
                return SendExpectString(Commands.Set, key.ToUtf8Bytes(), value, Commands.Ex, expirySeconds.ToUtf8Bytes(), entryExists) == OK;
            if (expiryMs > 0)
                return SendExpectString(Commands.Set, key.ToUtf8Bytes(), value, Commands.Px, expiryMs.ToUtf8Bytes(), entryExists) == OK;
            
            return SendExpectString(Commands.Set, key.ToUtf8Bytes(), value, entryExists) == OK;
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

			return SendExpectLong(Commands.Persist, key.ToUtf8Bytes()) == Success;
		}

    	public void PSetEx(string key, long expireInMs, byte[] value)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

            SendExpectSuccess(Commands.PSetEx, key.ToUtf8Bytes(), expireInMs.ToUtf8Bytes(), value);
		}

    	public long SetNX(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            return SendExpectLong(Commands.SetNx, key.ToUtf8Bytes(), value);
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

			return SendExpectLong(keysAndValues) == Success;
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

        public long Exists(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Exists, key.ToUtf8Bytes());
        }

        public long Del(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Del, key.ToUtf8Bytes());
        }

        public long Del(params string[] keys)
        {
            if (keys == null)
                throw new ArgumentNullException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.Del, keys);
            return SendExpectLong(cmdWithArgs);
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

        public long IncrBy(string key, long count)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            return SendExpectLong(Commands.IncrBy, key.ToUtf8Bytes(), count.ToUtf8Bytes());
        }

    	public double IncrByFloat(string key, double incrBy)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectDouble(Commands.IncrByFloat, key.ToUtf8Bytes(), incrBy.ToUtf8Bytes());
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

        public long Append(string key, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Append, key.ToUtf8Bytes(), value);
        }

    	public byte[] GetRange(string key, int fromIndex, int toIndex)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectData(Commands.GetRange, key.ToUtf8Bytes(), fromIndex.ToUtf8Bytes(), toIndex.ToUtf8Bytes());
		}

    	public long SetRange(string key, int offset, byte[] value)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.SetRange, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value);
		}

    	public long GetBit(string key, int offset)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.GetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes());
        }

        public long SetBit(string key, int offset, int value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            if (value > 1 || value < 0)
                throw  new ArgumentException("value is out of range");

            return SendExpectLong(Commands.SetBit, key.ToUtf8Bytes(), offset.ToUtf8Bytes(), value.ToUtf8Bytes());
        }

        public long BitCount(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.BitCount, key.ToUtf8Bytes());
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

			return SendExpectLong(Commands.RenameNx, oldKeyname.ToUtf8Bytes(), newKeyname.ToUtf8Bytes()) == Success;
		}

    	public bool Expire(string key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

			return SendExpectLong(Commands.Expire, key.ToUtf8Bytes(), seconds.ToUtf8Bytes()) == Success;
        }

		public bool PExpire(string key, long ttlMs)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.PExpire, key.ToUtf8Bytes(), ttlMs.ToUtf8Bytes()) == Success;
		}

    	public bool ExpireAt(string key, long unixTime)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.ExpireAt, key.ToUtf8Bytes(), unixTime.ToUtf8Bytes()) == Success;
        }

    	public bool PExpireAt(string key, long unixTimeMs)
    	{
			if (key == null)
				throw new ArgumentNullException("key");

			return SendExpectLong(Commands.PExpireAt, key.ToUtf8Bytes(), unixTimeMs.ToUtf8Bytes()) == Success;
		}

    	public long Ttl(string key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Ttl, key.ToUtf8Bytes());
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

        public RedisText Role()
        {
            return SendExpectComplexResponse(Commands.Role).ToRedisText();
        }

        public string ClientGetName()
        {
            return SendExpectString(Commands.Client, Commands.GetName);
        }

        public void ClientSetName(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Name cannot be null or empty");

            if (name.Contains(" "))
                throw new ArgumentException("Name cannot contain spaces");

            SendExpectSuccess(Commands.Client, Commands.SetName, name.ToUtf8Bytes());
        }

        public void ClientPause(int timeOutMs)
        {
            SendExpectSuccess(Commands.Client, Commands.Pause, timeOutMs.ToUtf8Bytes());
        }

        public byte[] ClientList()
        {
            return SendExpectData(Commands.Client, Commands.List);
        }

        public void ClientKill(string clientAddr)
        {
            SendExpectSuccess(Commands.Client, Commands.Kill, clientAddr.ToUtf8Bytes());
        }

        public long ClientKill(string addr = null, string id = null, string type = null, string skipMe = null)
        {
            var cmdWithArgs = new List<byte[]>
           	{
           		Commands.Client, Commands.Kill,
           	};

            if (addr != null)
            {
                cmdWithArgs.Add(Commands.Addr);
                cmdWithArgs.Add(addr.ToUtf8Bytes());
            }

            if (id != null)
            {
                cmdWithArgs.Add(Commands.Id);
                cmdWithArgs.Add(id.ToUtf8Bytes());
            }

            if (type != null)
            {
                cmdWithArgs.Add(Commands.Type);
                cmdWithArgs.Add(type.ToUtf8Bytes());
            }

            if (skipMe != null)
            {
                cmdWithArgs.Add(Commands.SkipMe);
                cmdWithArgs.Add(skipMe.ToUtf8Bytes());
            }

            return SendExpectLong(cmdWithArgs.ToArray());
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

        public ScanResult Scan(ulong cursor, int count = 10, string match = null)
        {
            if (match == null)
                return SendExpectScanResult(Commands.Scan, cursor.ToUtf8Bytes(),
                                            Commands.Count, count.ToUtf8Bytes());

            return SendExpectScanResult(Commands.Scan, cursor.ToUtf8Bytes(),
                                        Commands.Match, match.ToUtf8Bytes(),
                                        Commands.Count, count.ToUtf8Bytes());
        }

        public ScanResult SScan(string setId, ulong cursor, int count = 10, string match = null)
        {
            if (match == null)
            {
                return SendExpectScanResult(Commands.SScan, setId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                            Commands.Count, count.ToUtf8Bytes());
            }

            return SendExpectScanResult(Commands.SScan, setId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                        Commands.Match, match.ToUtf8Bytes(),
                                        Commands.Count, count.ToUtf8Bytes());
        }

        public ScanResult ZScan(string setId, ulong cursor, int count = 10, string match = null)
        {
            if (match == null)
            {
                return SendExpectScanResult(Commands.ZScan, setId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                            Commands.Count, count.ToUtf8Bytes());
            }

            return SendExpectScanResult(Commands.ZScan, setId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                        Commands.Match, match.ToUtf8Bytes(),
                                        Commands.Count, count.ToUtf8Bytes());
        }

        public ScanResult HScan(string hashId, ulong cursor, int count = 10, string match = null)
        {
            if (match == null)
            {
                return SendExpectScanResult(Commands.HScan, hashId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                            Commands.Count, count.ToUtf8Bytes());
            }

            return SendExpectScanResult(Commands.HScan, hashId.ToUtf8Bytes(), cursor.ToUtf8Bytes(),
                                        Commands.Match, match.ToUtf8Bytes(),
                                        Commands.Count, count.ToUtf8Bytes());
        }


        internal ScanResult SendExpectScanResult(byte[] cmd, params byte[][] args)
        {
            var cmdWithArgs = MergeCommandWithArgs(cmd, args);
            var multiData = SendExpectDeeplyNestedMultiData(cmdWithArgs);
            var counterBytes = (byte[]) multiData[0];

            var ret = new ScanResult {
                Cursor = ulong.Parse(counterBytes.FromUtf8Bytes()),
                Results = new List<byte[]>()
            };
            var keysBytes = (object[])multiData[1];

            foreach (var keyBytes in keysBytes)
            {
                ret.Results.Add((byte[])keyBytes);
            }

            return ret;
        }

        public bool PfAdd(string key, params byte[][] elements)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfAdd, key.ToUtf8Bytes(), elements);
            return SendExpectLong(cmdWithArgs) == 1;
        }

        public long PfCount(string key)
        {
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfCount, key.ToUtf8Bytes());
            return SendExpectLong(cmdWithArgs);
        }

        public void PfMerge(string toKeyId, params string[] fromKeys)
        {
            var fromKeyBytes = fromKeys.Map(x => x.ToUtf8Bytes()).ToArray();
            var cmdWithArgs = MergeCommandWithArgs(Commands.PfMerge, toKeyId.ToUtf8Bytes(), fromKeyBytes);
            SendExpectSuccess(cmdWithArgs);
        }

        #endregion


        #region Set Operations

        public byte[][] SMembers(string setId)
        {
            return SendExpectMultiData(Commands.SMembers, setId.ToUtf8Bytes());
        }

        public long SAdd(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectLong(Commands.SAdd, setId.ToUtf8Bytes(), value);
        }

        public long SAdd(string setId, byte[][] values)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (values == null)
                throw new ArgumentNullException("values");
            if (values.Length == 0)
                throw new ArgumentException("values");

            var cmdWithArgs = MergeCommandWithArgs(Commands.SAdd, setId.ToUtf8Bytes(), values);
            return SendExpectLong(cmdWithArgs);
        }

        public long SRem(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectLong(Commands.SRem, setId.ToUtf8Bytes(), value);
        }

        public long SRem(string setId, byte[][] values)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (values == null)
                throw new ArgumentNullException("values");
            if (values.Length == 0)
                throw new ArgumentException("values");

            var cmdWithArgs = MergeCommandWithArgs(Commands.SRem, setId.ToUtf8Bytes(), values);
            return SendExpectLong(cmdWithArgs);
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

        public long SCard(string setId)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.SCard, setId.ToUtf8Bytes());
        }

        public long SIsMember(string setId, byte[] value)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.SIsMember, setId.ToUtf8Bytes(), value);
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

        public byte[][] SRandMember(string setId, int count)
        {
            return SendExpectMultiData(Commands.SRandMember, setId.ToUtf8Bytes(), count.ToUtf8Bytes());
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

        public long RPush(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectLong(Commands.RPush, listId.ToUtf8Bytes(), value);
        }

        public long RPush(string listId, byte[][] values)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");
            if (values == null)
                throw new ArgumentNullException("values");
            if (values.Length == 0)
                throw new ArgumentException("values");

            var cmdWithArgs = MergeCommandWithArgs(Commands.RPush, listId.ToUtf8Bytes(), values);
            return SendExpectLong(cmdWithArgs);
        }

        public long RPushX(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectLong(Commands.RPushX, listId.ToUtf8Bytes(), value);
        }

    	public long LPush(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectLong(Commands.LPush, listId.ToUtf8Bytes(), value);
        }

        public long LPush(string listId, byte[][] values)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");
            if (values == null)
                throw new ArgumentNullException("values");
            if (values.Length == 0)
                throw new ArgumentException("values");

            var cmdWithArgs = MergeCommandWithArgs(Commands.LPush, listId.ToUtf8Bytes(), values);
            return SendExpectLong(cmdWithArgs);
        }

        public long LPushX(string listId, byte[] value)
        {
            AssertListIdAndValue(listId, value);

            return SendExpectLong(Commands.LPushX, listId.ToUtf8Bytes(), value);
        }

    	public void LTrim(string listId, int keepStartingFrom, int keepEndingAt)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            SendExpectSuccess(Commands.LTrim, listId.ToUtf8Bytes(), keepStartingFrom.ToUtf8Bytes(), keepEndingAt.ToUtf8Bytes());
        }

        public long LRem(string listId, int removeNoOfMatches, byte[] value)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectLong(Commands.LRem, listId.ToUtf8Bytes(), removeNoOfMatches.ToUtf8Bytes(), value);
        }

        public long LLen(string listId)
        {
            if (listId == null)
                throw new ArgumentNullException("listId");

            return SendExpectLong(Commands.LLen, listId.ToUtf8Bytes());
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

        public object[] Sentinel(string command, string master = null)
        {
            if (command == null)
                throw new ArgumentNullException("command");

            var args = new List<byte[]>()
            {
                Commands.Sentinel,
                command.ToUtf8Bytes()
            };

            if (master != null)
            {
                args.Add(master.ToUtf8Bytes());
            }
            return SendExpectDeeplyNestedMultiData(args.ToArray());
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

		public long ZAdd(string setId, double score, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectLong(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToFastUtf8Bytes(), value);
		}

		public long ZAdd(string setId, long score, byte[] value)
		{
			AssertSetIdAndValue(setId, value);

			return SendExpectLong(Commands.ZAdd, setId.ToUtf8Bytes(), score.ToUtf8Bytes(), value);
		}

        public long ZAdd(string setId, List<KeyValuePair<byte[], double>> pairs)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (pairs == null)
                throw new ArgumentNullException("pairs");
            if (pairs.Count == 0)
                throw new ArgumentOutOfRangeException("pairs");

            var mergedBytes = new byte[2 + pairs.Count * 2][];
            mergedBytes[0] = Commands.ZAdd;
            mergedBytes[1] = setId.ToUtf8Bytes();
            for (var i = 0; i < pairs.Count; i++)
            {
                mergedBytes[i * 2 + 2] = pairs[i].Value.ToFastUtf8Bytes();
                mergedBytes[i * 2 + 3] = pairs[i].Key;
            }
            return SendExpectLong(mergedBytes);
        }

        public long ZAdd(string setId, List<KeyValuePair<byte[], long>> pairs)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (pairs == null)
                throw new ArgumentNullException("pairs");
            if (pairs.Count == 0)
                throw new ArgumentOutOfRangeException("pairs");

            var mergedBytes = new byte[2 + pairs.Count * 2][];
            mergedBytes[0] = Commands.ZAdd;
            mergedBytes[1] = setId.ToUtf8Bytes();
            for (var i = 0; i < pairs.Count; i++)
            {
                mergedBytes[i * 2 + 2] = pairs[i].Value.ToUtf8Bytes();
                mergedBytes[i * 2 + 3] = pairs[i].Key;
            }
            return SendExpectLong(mergedBytes);
        }

        public long ZRem(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectLong(Commands.ZRem, setId.ToUtf8Bytes(), value);
        }

        public long ZRem(string setId, byte[][] values)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");
            if (values == null)
                throw new ArgumentNullException("values");
            if (values.Length == 0)
                throw new ArgumentException("values");

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZRem, setId.ToUtf8Bytes(), values);
            return SendExpectLong(cmdWithArgs);
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

        public long ZRank(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectLong(Commands.ZRank, setId.ToUtf8Bytes(), value);
        }

        public long ZRevRank(string setId, byte[] value)
        {
            AssertSetIdAndValue(setId, value);

            return SendExpectLong(Commands.ZRevRank, setId.ToUtf8Bytes(), value);
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

        public long ZRemRangeByRank(string setId, int min, int max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.ZRemRangeByRank, setId.ToUtf8Bytes(),
                min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

		public long ZRemRangeByScore(string setId, double fromScore, double toScore)
		{
			if (setId == null)
				throw new ArgumentNullException("setId");

			return SendExpectLong(Commands.ZRemRangeByScore, setId.ToUtf8Bytes(),
				fromScore.ToFastUtf8Bytes(), toScore.ToFastUtf8Bytes());
		}

		public long ZRemRangeByScore(string setId, long fromScore, long toScore)
		{
			if (setId == null)
				throw new ArgumentNullException("setId");

			return SendExpectLong(Commands.ZRemRangeByScore, setId.ToUtf8Bytes(),
				fromScore.ToUtf8Bytes(), toScore.ToUtf8Bytes());
		}

        public long ZCard(string setId)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.ZCard, setId.ToUtf8Bytes());
        }

        public long ZCount(string setId, double min, double max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public long ZCount(string setId, long min, long max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(Commands.ZCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public double ZScore(string setId, byte[] value)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectDouble(Commands.ZScore, setId.ToUtf8Bytes(), value);
        }

        public long ZUnionStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZUnionStore, setIdsList.ToArray());
            return SendExpectLong(cmdWithArgs);
        }

        public long ZUnionStore(string intoSetId, string[] setIds, string[] args)
        {
            var totalArgs = new List<string>(setIds);
            totalArgs.Insert(0, setIds.Length.ToString());
            totalArgs.Insert(0, intoSetId);
            totalArgs.AddRange(args);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZUnionStore, totalArgs.ToArray());
            return SendExpectLong(cmdWithArgs);
        }

        public long ZInterStore(string intoSetId, params string[] setIds)
        {
            var setIdsList = new List<string>(setIds);
            setIdsList.Insert(0, setIds.Length.ToString());
            setIdsList.Insert(0, intoSetId);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZInterStore, setIdsList.ToArray());
            return SendExpectLong(cmdWithArgs);
        }

        public long ZInterStore(string intoSetId, string[] setIds, string[] args)
        {
            var totalArgs = new List<string>(setIds);
            totalArgs.Insert(0, setIds.Length.ToString());
            totalArgs.Insert(0, intoSetId);
            totalArgs.AddRange(args);

            var cmdWithArgs = MergeCommandWithArgs(Commands.ZInterStore, totalArgs.ToArray());
            return SendExpectLong(cmdWithArgs);
        }

        public byte[][] ZRangeByLex(string setId, string min, string max, int? skip=null, int? take=null)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            var cmdWithArgs = new List<byte[]>
           	{
           		Commands.ZRangeByLex, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes()
           	};

            if (skip.HasValue || take.HasValue)
            {
                cmdWithArgs.Add(Commands.Limit);
                cmdWithArgs.Add(skip.GetValueOrDefault(0).ToUtf8Bytes());
                cmdWithArgs.Add(take.GetValueOrDefault(0).ToUtf8Bytes());
            }

            return SendExpectMultiData(cmdWithArgs.ToArray());
        }

        public long ZLexCount(string setId, string min, string max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(
                Commands.ZLexCount, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
        }

        public long ZRemRangeByLex(string setId, string min, string max)
        {
            if (setId == null)
                throw new ArgumentNullException("setId");

            return SendExpectLong(
                Commands.ZRemRangeByLex, setId.ToUtf8Bytes(), min.ToUtf8Bytes(), max.ToUtf8Bytes());
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

        public long HSet(string hashId, byte[] key, byte[] value)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HSet, hashId.ToUtf8Bytes(), key, value);
        }

        public long HSetNX(string hashId, byte[] key, byte[] value)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HSetNx, hashId.ToUtf8Bytes(), key, value);
        }

        public void HMSet(string hashId, byte[][] keys, byte[][] values)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");

            var cmdArgs = MergeCommandWithKeysAndValues(Commands.HMSet, hashId.ToUtf8Bytes(), keys, values);

            SendExpectSuccess(cmdArgs);
        }

        public long HIncrby(string hashId, byte[] key, int incrementBy)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

        public long HIncrby(string hashId, byte[] key, long incrementBy)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HIncrBy, hashId.ToUtf8Bytes(), key, incrementBy.ToString().ToUtf8Bytes());
        }

    	public double HIncrbyFloat(string hashId, byte[] key, double incrementBy)
    	{
			AssertHashIdAndKey(hashId, key);

			return SendExpectDouble(Commands.HIncrByFloat, hashId.ToUtf8Bytes(), key, incrementBy.ToString(CultureInfo.InvariantCulture).ToUtf8Bytes());
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

        public long HDel(string hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HDel, hashId.ToUtf8Bytes(), key);
        }

        public long HDel(string hashId, byte[][] keys)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");
            if (keys == null)
                throw new ArgumentNullException("keys");
            if (keys.Length == 0)
                throw new ArgumentException("keys");

            var cmdWithArgs = MergeCommandWithArgs(Commands.HDel, hashId.ToUtf8Bytes(), keys);
            return SendExpectLong(cmdWithArgs);
        }
        public long HExists(string hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HExists, hashId.ToUtf8Bytes(), key);
        }

        public long HLen(string hashId)
        {
            if (string.IsNullOrEmpty(hashId))
                throw new ArgumentNullException("hashId");

            return SendExpectLong(Commands.HLen, hashId.ToUtf8Bytes());
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

        public long Publish(string toChannel, byte[] message)
        {
            return SendExpectLong(Commands.Publish, toChannel.ToUtf8Bytes(), message);
        }

        public byte[][] ReceiveMessages()
        {
            return ReadMultiData();
        }

        public IRedisSubscription CreateSubscription()
        {
            return new RedisSubscription(this);
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

        public bool IsManagedClient
        {
            get { return ClientManager != null; }
        }

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
                if (sslStream != null)
                    sslStream.Close();
            }
            catch { }
            try
            {
                if (socket != null)
                    socket.Close();
            }
            catch { }

            Bstream = null;
            sslStream = null;
            socket = null;
        }
    }
}
