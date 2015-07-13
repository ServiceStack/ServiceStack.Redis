using System;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Logging;
using ServiceStack.Text;
using Timer = System.Timers.Timer;

namespace ServiceStack.Redis.Tests
{
    [Ignore("Ignore Sentinel Integration Tests")]
    [TestFixture, Category("Integration")]
    public class RedisSentinelTests
        : RedisSentinelTestBase
    {
        [TestFixtureSetUp]
        public void OnBeforeTestFixture()
        {
            StartAllRedisServers();
            StartAllRedisSentinels();
            LogManager.LogFactory = new ConsoleLogFactory(debugEnabled:true);
        }

        [TestFixtureTearDown]
        public void OnAfterTestFixture()
        {
            ShutdownAllRedisSentinels();
            ShutdownAllRedisServers();
        }

        protected RedisClient RedisSentinel;

        [SetUp]
        public void OnBeforeEachTest()
        {
            var parts = SentinelHosts[0].SplitOnFirst(':');
            RedisSentinel = new RedisClient(parts[0], int.Parse(parts[1]));
        }

        [TearDown]
        public void OnAfterEachTest()
        {
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
            var masters = RedisSentinel.SentinelMasters();
            masters.PrintDump();

            Assert.That(masters.Count, Is.GreaterThan(0));
        }

        [Test]
        public void Can_Get_Sentinel_Master()
        {
            var master = RedisSentinel.SentinelMaster(MasterName);
            master.PrintDump();

            var host = "{0}:{1}".Fmt(master["ip"], master["port"]);
            Assert.That(master["name"], Is.EqualTo(MasterName));
            Assert.That(host, Is.EqualTo(MasterHosts[0]));
        }

        [Test]
        public void Can_Get_Sentinel_Slaves()
        {
            var slaves = RedisSentinel.SentinelSlaves(MasterName);
            slaves.PrintDump();

            Assert.That(slaves.Count, Is.GreaterThan(0));
        }

        [Test]
        public void Can_Get_Master_Addr()
        {
            var addr = RedisSentinel.SentinelGetMasterAddrByName(MasterName);

            string host = addr[0];
            string port = addr[1];
            var hostString = "{0}:{1}".Fmt(host, port);

            // IP of localhost
            Assert.That(hostString, Is.EqualTo(MasterHosts[0]));
        }

        [Test]
        public void Can_Get_Redis_ClientsManager()
        {
            using (var sentinel = CreateSentinel())
            {
                var clientsManager = sentinel.Start();
                using (var client = clientsManager.GetClient())
                {
                    Assert.That(client.GetHostString(), Is.EqualTo(MasterHosts[0]));
                }
            }
        }

        [Test]
        public void Can_specify_Timeout_on_RedisManager()
        {
            using (var sentinel = CreateSentinel())
            {
                sentinel.RedisManagerFactory = (masters, slaves) => new PooledRedisClientManager(masters, slaves) { IdleTimeOutSecs = 20 };

                using (var clientsManager = (PooledRedisClientManager)sentinel.Start())
                using (var client = clientsManager.GetClient())
                {
                    Assert.That(clientsManager.IdleTimeOutSecs, Is.EqualTo(20));
                    Assert.That(((RedisNativeClient)client).IdleTimeOutSecs, Is.EqualTo(20));
                }
            }
        }

        [Test]
        public void Can_specify_db_on_RedisSentinel()
        {
            using (var sentinel = CreateSentinel())
            {
                sentinel.HostFilter = host => "{0}?db=1".Fmt(host);
 
                using (var clientsManager = sentinel.Start())
                using (var client = clientsManager.GetClient())
                {
                    Assert.That(client.Db, Is.EqualTo(1));
                }
            }
        }

        [Ignore, Test]
        public void Run_sentinel_for_10_minutes()
        {
            ILog log = LogManager.GetLogger(GetType());

            using (var sentinel = CreateSentinel())
            {
                sentinel.OnFailover = manager => "Redis Managers Failed Over to new hosts".Print();
                sentinel.OnWorkerError = ex => "Worker error: {0}".Print(ex);
                sentinel.OnSentinelMessageReceived = (channel, msg) => "Received '{0}' on channel '{1}' from Sentinel".Print(channel, msg);

                using (var redisManager = sentinel.Start())
                {
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
                            log.InfoFormat("Set key {0} in read/write client", key);
                            redis.SetValue(key, "value" + 1);
                        }

                        using (var redis = redisManager.GetClient())
                        {
                            log.InfoFormat("Get key {0} in read-only client...", key);
                            var value = redis.GetValue(key);
                            log.InfoFormat("{0} = {1}", key, value);
                        }
                    };
                }
            }

            Thread.Sleep(TimeSpan.FromMinutes(10));
        }

        [Test]
        public void Defaults_to_default_sentinel_port()
        {
            var sentinelEndpoint = "127.0.0.1".ToRedisEndpoint(defaultPort: RedisNativeClient.DefaultPortSentinel);
            Assert.That(sentinelEndpoint.Port, Is.EqualTo(RedisNativeClient.DefaultPortSentinel));
        }
    }
}
