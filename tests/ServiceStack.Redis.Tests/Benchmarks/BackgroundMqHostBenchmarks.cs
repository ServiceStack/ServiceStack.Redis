using System;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Redis.Tests.Benchmarks
{
	[TestFixture]
	public class BackgroundMqHostBenchmarks
	{
		public class Incr
		{
			public int Value { get; set; }
		}

		private static BackgroundThreadMqHost CreateMqHost()
		{
			return CreateMqHost(2);
		}

		private static BackgroundThreadMqHost CreateMqHost(int noOfRetries)
		{
			var redisFactory = new BasicRedisClientManager();
			try
			{
				redisFactory.Exec(redis => redis.FlushAll());
			}
			catch (RedisException rex)
			{
				Console.WriteLine("WARNING: Redis not started? \n" + rex.Message);
			}
			var mqHost = new BackgroundThreadMqHost(redisFactory, noOfRetries, null);
			return mqHost;
		}

		[Test]
		public void Can_receive_and_process_same_reply_responses()
		{
			var mqHost = CreateMqHost();
			var called = 0;

			mqHost.RegisterHandler<Incr>(m => {
				called++;
				return new Incr { Value = m.Body.Value + 1 };
			});

			mqHost.Start();

			var mqClient = mqHost.CreateMessageQueueClient();
			mqClient.Publish(new Incr { Value = 1 });

			Thread.Sleep(10000);

			Console.WriteLine("Times called: " + called);
		}

	}
}