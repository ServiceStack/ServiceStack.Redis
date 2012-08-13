using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Redis.Generic;

namespace ServiceStack.Redis.Tests.Generic
{
	[TestFixture, Category("Integration")]
	public class RedisTypedClientTests
	{
		public class CacheRecord
		{
			public CacheRecord()
			{
				this.Children = new List<CacheRecordChild>();
			}

			public string Id { get; set; }
			public List<CacheRecordChild> Children { get; set; }
		}

		public class CacheRecordChild
		{
			public string Id { get; set; }
			public string Data { get; set; }
		}

		protected RedisClient Redis;
		protected IRedisTypedClient<CacheRecord> RedisTyped;

		protected void Log(string fmt, params object[] args)
		{
			Debug.WriteLine("{0}", string.Format(fmt, args).Trim());
		}

		[SetUp]
		public virtual void OnBeforeEachTest()
		{
			if (Redis != null) Redis.Dispose();
			Redis = new RedisClient(TestConfig.SingleHost);
			Redis.FlushDb();
			RedisTyped = Redis.As<CacheRecord>();
		}


		[Test]
		public void Can_Expire()
		{
			var cachedRecord = new CacheRecord
			{
				Id = "key",
				Children = {
					new CacheRecordChild { Id = "childKey", Data = "data" }
				}
			};

			RedisTyped.Store(cachedRecord);
			RedisTyped.ExpireIn("key", TimeSpan.FromSeconds(1));
			Assert.That(RedisTyped.GetById("key"), Is.Not.Null);
			Thread.Sleep(2000);
			Assert.That(RedisTyped.GetById("key"), Is.Null);
		}

		[Test]
		public void Can_ExpireAt()
		{
			var cachedRecord = new CacheRecord
			{
				Id = "key",
				Children = {
					new CacheRecordChild { Id = "childKey", Data = "data" }
				}
			};

			RedisTyped.Store(cachedRecord);

			var in1Sec = DateTime.Now.AddSeconds(1);

			RedisTyped.ExpireAt("key", in1Sec);

			Assert.That(RedisTyped.GetById("key"), Is.Not.Null);
			Thread.Sleep(2000);
			Assert.That(RedisTyped.GetById("key"), Is.Null);
		}
	}

}