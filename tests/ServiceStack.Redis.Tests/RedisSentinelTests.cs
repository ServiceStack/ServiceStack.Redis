using System;
using System.Linq;
using System.Text;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Logging;
using ServiceStack.Text;
using Timer = System.Timers.Timer;

namespace ServiceStack.Redis.Tests
{
    [Ignore("Reenable when CI has Sentinel")]
    [TestFixture, Category("Integration")]
    public class RedisSentinelTests
        : RedisClientTestsBase
    {
        protected RedisClient RedisSentinel;


        public override void OnBeforeEachTest()
        {
            base.OnBeforeEachTest();

            RedisSentinel = new RedisClient(TestConfig.SentinelHost, TestConfig.RedisSentinelPort);
        }


        public override void TearDown()
        {
            base.TearDown();

            RedisSentinel.Dispose();
        }


        [Test]
        public void Can_Ping_Sentinel()
        {
            Assert.True(RedisSentinel.Ping());
        }

        [Test]
        public void Can_Get_Sentinel_Masters()
        {
            object[] masters = RedisSentinel.Sentinel("masters");

            Assert.AreEqual(masters.Count(), TestConfig.MasterHosts.Count());
        }

        [Test]
        public void Can_Get_Sentinel_Slaves()
        {
            object[] slaves = RedisSentinel.Sentinel("slaves", TestConfig.MasterName);

            Assert.That(slaves.Count(), Is.GreaterThan(0));
        }

        [Test]
        public void Can_Get_Master_Addr()
        {
            object[] addr = RedisSentinel.Sentinel("get-master-addr-by-name", TestConfig.MasterName);

            string host = Encoding.UTF8.GetString((byte[])addr[0]);
            string port = Encoding.UTF8.GetString((byte[])addr[1]);

            // IP of localhost
            Assert.That(host, Is.EqualTo("127.0.0.1").Or.EqualTo(TestConfig.SentinelHost));
            Assert.AreEqual(port, TestConfig.RedisPort.ToString());
        }

        [Test]
        public void Can_Get_Redis_ClientsManager()
        {
            var sentinel = new RedisSentinel(new[] { "{0}:{1}".Fmt(TestConfig.SentinelHost, TestConfig.RedisSentinelPort) }, TestConfig.MasterName);

            var clientsManager = sentinel.Setup();
            var client = clientsManager.GetClient();

            Assert.That(client.Host, Is.EqualTo("127.0.0.1").Or.EqualTo(TestConfig.SentinelHost));
            Assert.AreEqual(client.Port, TestConfig.RedisPort);

            client.Dispose();
            sentinel.Dispose();
        }

        [Test]
        public void Can_specify_Timeout_on_RedisManager()
        {
            var sentinel = new RedisSentinel(new[] { "{0}:{1}".Fmt(TestConfig.SentinelHost, TestConfig.RedisSentinelPort) }, TestConfig.MasterName)
            {
                RedisManagerFactory =
                {
                    OnInit = r =>
                    {
                        ((PooledRedisClientManager)r).IdleTimeOutSecs = 20;
                    }
                }
            };

            using (var clientsManager = (PooledRedisClientManager)sentinel.Setup())
            using (var client = clientsManager.GetClient())
            {
                Assert.That(clientsManager.IdleTimeOutSecs, Is.EqualTo(20));
                Assert.That(((RedisNativeClient)client).IdleTimeOutSecs, Is.EqualTo(20));
            }
        }

        [Test]
        public void Can_specify_db_on_RedisSentinel()
        {
            var sentinelHosts = new[] { "{0}:{1}".Fmt(TestConfig.SentinelHost, TestConfig.RedisSentinelPort) };
            var sentinel = new RedisSentinel(sentinelHosts, TestConfig.MasterName)
            {
                HostFilter = host => "{0}?db=1".Fmt(host)
            };

            using (var clientsManager = sentinel.Setup())
            using (var client = clientsManager.GetClient())
            {
                Assert.That(client.Db, Is.EqualTo(1));
            }
        }

        [Test]
        public void Run_sentinel_for_10_minutes()
        {
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            var sentinelHost = "{0}:{1}".Fmt(TestConfig.SentinelHost, TestConfig.RedisSentinelPort);
            var sentinel = new RedisSentinel(sentinelHost, TestConfig.MasterName)
            {
                OnFailover = manager =>
                {
                    "Redis Managers Failed Over to new hosts".Print();
                },
                OnWorkerError = ex =>
                {
                    "Worker error: {0}".Print(ex);
                },
                OnSentinelMessageReceived = (channel, msg) =>
                {
                    "Received '{0}' on channel '{1}' from Sentinel".Print(channel, msg);
                },
            };

            var redisManager = sentinel.Start();

            var aTimer = new Timer
            {
                Interval = 1000,
                Enabled = true
            };
            aTimer.Elapsed += (sender, args) =>
            {
                "Incrementing key".Print();

                string key = null;
                using (var redis = redisManager.GetClient())
                {
                    var counter = redis.Increment("key", 1);
                    key = "key" + counter;
                    "Set key {0} in read/write client".Print(key);
                    redis.SetEntry(key, "value" + 1);
                }

                using (var redis = redisManager.GetClient())
                {
                    "Get key {0} in read-only client...".Print(key);
                    var value = redis.GetEntry(key);
                    "{0} = {1}".Print(key, value);
                }
            };

            Thread.Sleep(TimeSpan.FromMinutes(10));
        }
    }
}
