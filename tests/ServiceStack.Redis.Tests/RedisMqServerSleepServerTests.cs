﻿using System;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture, Category("Integration")]
    public class RedisMqServerSleepServerTests
    {
        public class Counters
        {
            public int Sleep0 { get; set; }
            public int Sleep10 { get; set; }
            public int Sleep100 { get; set; }
            public int Sleep1000 { get; set; }
        }

        class Sleep0
        {
            public int Id { get; set; }
        }
        class Sleep10
        {
            public int Id { get; set; }
        }
        class Sleep100
        {
            public int Id { get; set; }
        }
        class Sleep1000
        {
            public int Id { get; set; }
        }

        readonly Counters counter = new Counters();

        RedisMqServer CreateServer()
        {
            using (var redis = new RedisClient())
                redis.FlushAll();

            var mqServer = new RedisMqServer(new BasicRedisClientManager());
            mqServer.RegisterHandler<Sleep0>(m => new Sleep0 { Id = counter.Sleep0++ });

            mqServer.RegisterHandler<Sleep10>(m => {
                Thread.Sleep(10);
                return new Sleep10 { Id = counter.Sleep10++ };
            });
            mqServer.RegisterHandler<Sleep100>(m => {
                Thread.Sleep(100);
                return new Sleep100 { Id = counter.Sleep100++ };
            });
            mqServer.RegisterHandler<Sleep1000>(m => {
                Thread.Sleep(1000);
                return new Sleep1000 { Id = counter.Sleep1000++ };
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

        private void RunFor(TimeSpan SleepFor)
        {
            var mqServer = CreateServer();

            mqServer.Start();

            using (var mqClient = mqServer.CreateMessageQueueClient())
            {
                mqClient.Publish(new Sleep0());
                mqClient.Publish(new Sleep10());
                mqClient.Publish(new Sleep100());
                mqClient.Publish(new Sleep1000());
            }

            Thread.Sleep(SleepFor);

            Debug.WriteLine(counter.Dump());

            Debug.WriteLine("Disposing...");
            mqServer.Dispose();

            Debug.WriteLine(counter.Dump());
        }
    }
}