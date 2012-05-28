using System;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisMqServerMultiServerTests
    {
        public class Counters
        {
            public int Wait0 { get; set; }
            public int Wait10 { get; set; }
            public int Wait100 { get; set; }
            public int Wait1000 { get; set; }
        }

        class Wait0
        {
            public int Id { get; set; }
        }
        class Wait10
        {
            public int Id { get; set; }
        }
        class Wait100
        {
            public int Id { get; set; }
        }
        class Wait1000
        {
            public int Id { get; set; }
        }

        readonly Counters counter = new Counters();

        RedisMqServer CreateServer()
        {
            using (var redis = new RedisClient())
                redis.FlushAll();

            var mqServer = new RedisMqServer(new BasicRedisClientManager());
            mqServer.RegisterHandler<Wait0>(m => new Wait0 { Id = counter.Wait0++ });

            mqServer.RegisterHandler<Wait10>(m => {
                Thread.Sleep(10);
                return new Wait10 { Id = counter.Wait10++ };
            });
            mqServer.RegisterHandler<Wait100>(m => {
                Thread.Sleep(100);
                return new Wait100 { Id = counter.Wait100++ };
            });
            mqServer.RegisterHandler<Wait1000>(m => {
                Thread.Sleep(1000);
                return new Wait1000 { Id = counter.Wait1000++ };
            });


            return mqServer;
        }

        [Test]
        public void Run_for_1_seconds()
        {
            RunFor(TimeSpan.FromSeconds(1));
        }
        
        [Test]
        public void Run_for_5_seconds()
        {
            RunFor(TimeSpan.FromSeconds(5));
        }

        [Test]
        public void Run_for_10_seconds()
        {
            RunFor(TimeSpan.FromSeconds(10));
        }

        [Test]
        public void Run_for_30_seconds()
        {
            RunFor(TimeSpan.FromSeconds(30));
        }

        private void RunFor(TimeSpan waitFor)
        {
            var mqServer = CreateServer();

            mqServer.Start();

            using (var mqClient = mqServer.CreateMessageQueueClient())
            {
                mqClient.Publish(new Wait0());
                mqClient.Publish(new Wait10());
                mqClient.Publish(new Wait100());
                mqClient.Publish(new Wait1000());
            }

            Thread.Sleep(waitFor);

            Console.WriteLine(counter.Dump());

            Console.WriteLine("Disposing...");
            mqServer.Dispose();

            Console.WriteLine(counter.Dump());
        }
    }
}