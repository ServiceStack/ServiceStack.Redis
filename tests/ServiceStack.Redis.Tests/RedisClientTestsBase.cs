using System;
using System.Diagnostics;
using System.Text;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
	[TestFixture, Category("Integration")]
	public class RedisClientTestsBase
	{
	    protected string CleanMask = null;
		protected RedisClient Redis;

		protected void Log(string fmt, params object[] args)
		{
			Debug.WriteLine("{0}", string.Format(fmt, args).Trim());
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp()
		{
			RedisClient.NewFactoryFn = () => new RedisClient(TestConfig.SingleHost);
		}

		[SetUp]
		public virtual void OnBeforeEachTest()
		{
			Redis = RedisClient.New();
		}

        [TearDown]
        public virtual void TearDown()
        {
            if (Redis.NamespacePrefix != null && CleanMask == null) CleanMask = Redis.NamespacePrefix + "*";
            if (CleanMask != null) Redis.SearchKeys(CleanMask).ForEach(t => Redis.Del(t));
            Redis.Dispose();
        }

        protected string PrefixedKey(string key)
        {
            return string.Concat(Redis.NamespacePrefix, key);
        }

		public RedisClient GetRedisClient()
		{
			var client = new RedisClient(TestConfig.SingleHost);
			return client;
		}

		public RedisClient CreateRedisClient()
		{
			var client = new RedisClient(TestConfig.SingleHost);
			return client;
		}

		public string GetString(byte[] stringBytes)
		{
			return Encoding.UTF8.GetString(stringBytes);
		}

		public byte[] GetBytes(string stringValue)
		{
			return Encoding.UTF8.GetBytes(stringValue);
		}
	}
}