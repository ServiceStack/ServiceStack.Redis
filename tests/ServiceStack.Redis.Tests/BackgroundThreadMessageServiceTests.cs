using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Common;
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

        private static void Publish_4_messages(IMessageQueueClient mqClient)
        {
            mqClient.Publish(new Reverse { Value = "Hello" });
            mqClient.Publish(new Reverse { Value = "World" });
            mqClient.Publish(new Reverse { Value = "ServiceStack" });
            mqClient.Publish(new Reverse { Value = "Redis" });
        }

        [Test]
        public void Does_process_messages_sent_before_it_was_started()
        {
            var reverseCalled = 0;

            var mqHost = CreateMqHost();
            mqHost.RegisterHandler<Reverse>(x => { reverseCalled++; return x.Body.Value.Reverse(); });

            var mqClient = mqHost.CreateMessageQueueClient();
            Publish_4_messages(mqClient);

            mqHost.Start();
            Thread.Sleep(3000);

            Assert.That(mqHost.GetStats().TotalMessagesProcessed, Is.EqualTo(4));
            Assert.That(reverseCalled, Is.EqualTo(4));

            mqHost.Dispose();
        }

        [Test]
        public void Does_process_all_messages_and_Starts_Stops_correctly_with_multiple_threads_racing()
        {
            var mqHost = CreateMqHost();

            var reverseCalled = 0;
            var rot13Called = 0;

            mqHost.RegisterHandler<Reverse>(x => { reverseCalled++; return x.Body.Value.Reverse(); });
            mqHost.RegisterHandler<Rot13>(x => { rot13Called++; return x.Body.Value.ToRot13(); });

            var mqClient = mqHost.CreateMessageQueueClient();
            mqClient.Publish(new Reverse { Value = "Hello" });
            mqClient.Publish(new Reverse { Value = "World" });
            mqClient.Publish(new Rot13 { Value = "ServiceStack" });

            mqHost.Start();
            Thread.Sleep(3000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Started"));
            Assert.That(mqHost.GetStats().TotalMessagesProcessed, Is.EqualTo(3));

            mqClient.Publish(new Reverse { Value = "Foo" });
            mqClient.Publish(new Rot13 { Value = "Bar" });

            10.Times(x => ThreadPool.QueueUserWorkItem(y => mqHost.Start()));
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Started"));

            5.Times(x => ThreadPool.QueueUserWorkItem(y => mqHost.Stop()));
            Thread.Sleep(1000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Stopped"));

            10.Times(x => ThreadPool.QueueUserWorkItem(y => mqHost.Start()));
            Thread.Sleep(3000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Started"));

            Console.WriteLine("\n" + mqHost.GetStats());

            Assert.That(mqHost.GetStats().TotalMessagesProcessed, Is.EqualTo(5));
            Assert.That(reverseCalled, Is.EqualTo(3));
            Assert.That(rot13Called, Is.EqualTo(2));

            mqHost.Dispose();
        }

        [Test]
        public void Only_allows_1_BgThread_to_run_at_a_time()
        {
            var mqHost = CreateMqHost();

            mqHost.RegisterHandler<Reverse>(x => x.Body.Value.Reverse());
            mqHost.RegisterHandler<Rot13>(x => x.Body.Value.ToRot13());

            5.Times(x => ThreadPool.QueueUserWorkItem(y => mqHost.Start()));
            Thread.Sleep(1000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Started"));
            Assert.That(mqHost.BgThreadCount, Is.EqualTo(1));

            10.Times(x => ThreadPool.QueueUserWorkItem(y => mqHost.Stop()));
            Thread.Sleep(1000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Stopped"));

            ThreadPool.QueueUserWorkItem(y => mqHost.Start());
            Thread.Sleep(1000);
            Assert.That(mqHost.GetStatus(), Is.EqualTo("Started"));

            Assert.That(mqHost.BgThreadCount, Is.EqualTo(2));

            Console.WriteLine(mqHost.GetStats());

            mqHost.Dispose();
        }

        [Test]
        public void Cannot_Start_a_Disposed_MqHost()
        {
            var mqHost = CreateMqHost();

            mqHost.RegisterHandler<Reverse>(x => x.Body.Value.Reverse());
            mqHost.Dispose();

            try
            {
                mqHost.Start();
                Assert.Fail("Should throw ObjectDisposedException");
            }
            catch (ObjectDisposedException) { }
        }

        [Test]
        public void Cannot_Stop_a_Disposed_MqHost()
        {
            var mqHost = CreateMqHost();

            mqHost.RegisterHandler<Reverse>(x => x.Body.Value.Reverse());
            mqHost.Start();
            Thread.Sleep(1000);

            mqHost.Dispose();

            try
            {
                mqHost.Stop();
                Assert.Fail("Should throw ObjectDisposedException");
            }
            catch (ObjectDisposedException) { }
        }
        
        public class AlwaysThrows
        {
            public string Value { get; set; }
        }

        [Test]
        public void Does_retry_messages_with_errors_by_RetryCount()
        {
            var retryCount = 3; 
            var totalRetries = 1 + retryCount; //in total, inc. first try

            var mqHost = CreateMqHost(retryCount);

            var reverseCalled = 0;
            var rot13Called = 0;
            
            mqHost.RegisterHandler<Reverse>(x => { reverseCalled++; return x.Body.Value.Reverse(); });
            mqHost.RegisterHandler<Rot13>(x => { rot13Called++; return x.Body.Value.ToRot13(); });
            mqHost.RegisterHandler<AlwaysThrows>(x => { throw new Exception("Always Throwing! " + x.Body.Value); });
            mqHost.Start();

            var mqClient = mqHost.CreateMessageQueueClient();
            mqClient.Publish(new AlwaysThrows { Value = "1st" });
            mqClient.Publish(new Reverse { Value = "Hello" });
            mqClient.Publish(new Reverse { Value = "World" });
            mqClient.Publish(new Rot13 { Value = "ServiceStack" });

            Thread.Sleep(3000);
            Assert.That(mqHost.GetStats().TotalMessagesFailed, Is.EqualTo(1 * totalRetries));
            Assert.That(mqHost.GetStats().TotalMessagesProcessed, Is.EqualTo(2 + 1));

            5.Times(x => mqClient.Publish(new AlwaysThrows { Value = "#" + x }));

            mqClient.Publish(new Reverse { Value = "Hello" });
            mqClient.Publish(new Reverse { Value = "World" });
            mqClient.Publish(new Rot13 { Value = "ServiceStack" });
            
            Thread.Sleep(5000);

            Console.WriteLine(mqHost.GetStatsDescription());

            Assert.That(mqHost.GetStats().TotalMessagesFailed, Is.EqualTo((1 + 5) * totalRetries));
            Assert.That(mqHost.GetStats().TotalMessagesProcessed, Is.EqualTo(6));

            Assert.That(reverseCalled, Is.EqualTo(2 + 2));
            Assert.That(rot13Called, Is.EqualTo(1 + 1));
        }

    }
}