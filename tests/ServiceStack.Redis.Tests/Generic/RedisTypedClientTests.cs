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
        : RedisClientTestsBase
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
            base.OnBeforeEachTest();

			if (Redis != null) Redis.Dispose();
			Redis = new RedisClient(TestConfig.SingleHost);
		    Redis.NamespacePrefix = "RedisTypedClientTests:";
			RedisTyped = Redis.As<CacheRecord>();
		}

        [TearDown]
        public virtual void TearDown()
        {
            Redis.SearchKeys(Redis.NamespacePrefix + "*").ForEach(t => Redis.Del(t));
        }

        [Test]
        public void Can_Store_with_Prefix()
        {
            var expected = new CacheRecord() {Id = "123"};
            RedisTyped.Store(expected);
            var current = Redis.Get<CacheRecord>("RedisTypedClientTests:urn:cacherecord:123");
            Assert.AreEqual(expected.Id, current.Id);
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

			var in2Secs = DateTime.Now.AddSeconds(2);

			RedisTyped.ExpireAt("key", in2Secs);

			Assert.That(RedisTyped.GetById("key"), Is.Not.Null);
			Thread.Sleep(3000);
			Assert.That(RedisTyped.GetById("key"), Is.Null);
		}

        [Test]
        public void Can_Delete_All_Items()
        {
            var cachedRecord = new CacheRecord
            {
                Id = "key",
                Children = {
					new CacheRecordChild { Id = "childKey", Data = "data" }
				}
            };

            RedisTyped.Store(cachedRecord);

            Assert.That(RedisTyped.GetById("key"), Is.Not.Null);

            RedisTyped.DeleteAll();

            Assert.That(RedisTyped.GetById("key"), Is.Null);

        }
	}

}