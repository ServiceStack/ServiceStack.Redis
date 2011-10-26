using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Logging;
using ServiceStack.Logging.Support.Logging;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
	[TestFixture]
	public class BackgroundThreadMessageServiceTests
	{
		public class Reverse
		{
			public string Value { get; set; }
		}

		public class Rot13
		{
			public string Value { get; set; }
		}

		[TestFixtureSetUp]
		public void TestFixtureSetUp()
		{
			LogManager.LogFactory = new ConsoleLogFactory();
		}

		private static BackgroundThreadMessageService CreateMqHost()
		{
			var redisFactory = new BasicRedisClientManager();
			var mqHost = new BackgroundThreadMessageService(redisFactory, 2, null);
			return mqHost;
		}

		[Test]
		public void Can_Fire_Messages()
		{
			var mqHost = CreateMqHost();
			var mqClient = mqHost.CreateMessageQueueClient();
			mqClient.Publish(new Reverse { Value = "Demis" });
			mqClient.Publish(new Reverse { Value = "Bellot" });
			Thread.Sleep(1000);
			mqClient.Publish(new Reverse { Value = "ServiceStack" });
			mqClient.Publish(new Reverse { Value = "Redis" });			
		}

		[Test]
		public void Message_does_fire_PubSub()
		{
			var redisFactory = new BasicRedisClientManager();
			var subscription = new RedisSubscription((IRedisNativeClient) redisFactory.GetClient());

			subscription.OnMessage = (ch, msg) => Console.WriteLine("{0} from {1}".Fmt(msg, ch));

			Action a = Can_Fire_Messages;
			a.BeginInvoke(null, null);
			subscription.SubscribeToChannels(QueueNames<Reverse>.In);

			//Thread.Sleep(30000);
		}

		[Test]
		public void Can_Start_BackgroundThreadMessageService()
		{
			var mqHost = CreateMqHost();

			mqHost.RegisterHandler<Reverse>(x => x.Body.Value.Reverse());
			mqHost.RegisterHandler<Rot13>(x => x.Body.Value.ToRot13());

			mqHost.Start();
			Thread.Sleep(TimeSpan.FromSeconds(1));

			var mqClient = mqHost.CreateMessageQueueClient();
			mqClient.Publish(new Reverse { Value = "Demis" });
			mqClient.Publish(new Rot13 { Value = "Bellot" });

			Console.WriteLine(mqHost.GetStats());

			for (var i=0; i > 10; i++) mqHost.Start();
			mqHost.Stop();
			for (var i=0; i > 10; i++) mqHost.Start();

			Console.WriteLine(mqHost.GetStats());

			Thread.Sleep(TimeSpan.FromSeconds(5));
		}
	}
}