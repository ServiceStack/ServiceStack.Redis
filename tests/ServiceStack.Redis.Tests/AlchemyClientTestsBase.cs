using System;
using System.Text;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
	[TestFixture]
	public class AlchemyClientTestsBase
	{
		protected RedisNativeClient.AlchemyNativeClient Alchemy;

		protected void Log(string fmt, params object[] args)
		{
			Console.WriteLine("{0}", string.Format(fmt, args).Trim());
		}

		[SetUp]
		public virtual void OnBeforeEachTest()
		{
			if (Alchemy != null) Alchemy.Dispose();
			Alchemy = new RedisNativeClient.AlchemyNativeClient(TestConfig.SingleHost);
			Alchemy.FlushDb();
		}

        public RedisNativeClient.AlchemyNativeClient GetRedisClient()
		{
            var client = new RedisNativeClient.AlchemyNativeClient(TestConfig.SingleHost);
			client.FlushDb();
			return client;
		}

        public RedisNativeClient.AlchemyNativeClient CreateRedisClient()
		{
            var client = new RedisNativeClient.AlchemyNativeClient(TestConfig.SingleHost);
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