using System;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Redis.Messaging;

namespace ServiceStack.Redis.Tests.Benchmarks
{
	[TestFixture]
	public class RedisMqHostBenchmarks
	{
		public class Incr
		{
			public int Value { get; set; }
		}

        public class IncrBlocking
        {
            public int Value { get; set; }
        }

		private static RedisMqHost CreateMqHost()
		{
			return CreateMqHost(2);
		}

		private static RedisMqHost CreateMqHost(int noOfRetries)
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
			var mqHost = new RedisMqHost(redisFactory, noOfRetries, null);
			return mqHost;
		}

        [Test]
        public void Can_receive_and_process_same_reply_responses()
        {
            var mqHost = CreateMqHost();
            var called = 0;

            mqHost.RegisterHandler<Incr>(m =>
            {
                called++;
                return new Incr { Value = m.GetBody().Value + 1 };
            });

            mqHost.Start();

            var mqClient = mqHost.CreateMessageQueueClient();
            mqClient.Publish(new Incr { Value = 1 });

            Thread.Sleep(10000);

            Console.WriteLine("Times called: " + called);
        }

        [Test]
        public void Can_receive_and_process_same_reply_responses_blocking()
        {
            var mqHost = CreateMqHost();
            var called = 0;

            mqHost.RegisterHandler<IncrBlocking>(m =>
            {
                called++;
                mqHost.CreateMessageQueueClient().Publish(new IncrBlocking { Value = m.GetBody().Value + 1 });
                Thread.Sleep(100);
                return null;
            });

            mqHost.Start();

            var mqClient = mqHost.CreateMessageQueueClient();
            mqClient.Publish(new IncrBlocking { Value = 1 });

            Thread.Sleep(10000);

            Console.WriteLine("Times called: " + called);
        }

	}
}