using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [Ignore("Reenable when CI has Sentinel")]
    [TestFixture, Category("Integration")]
    public class Redis3SentinelSetupTests
        : RedisClientTestsBase
    {
        static string[] SentinelHosts = new[]
        {
            "127.0.0.1:26380",
            "127.0.0.1:26381",
            "127.0.0.1:26382",
        };

        [Test]
        public void Can_connect_to_3SentinelSetup()
        {
            var sentinel = new RedisSentinel(SentinelHosts);

            var redisManager = sentinel.Start();

            using (var client = redisManager.GetClient())
            {
                client.FlushAll();

                client.SetEntry("Sentinel3Setup", "IntranetSentinel");

                var result = client.GetEntry("Sentinel3Setup");
                Assert.That(result, Is.EqualTo("IntranetSentinel"));
            }
        }

        static string[] GoogleCloudSentinelHosts = new[]
        {
            "146.148.77.31",
            "130.211.139.141",
            "107.178.218.53",
        };

        private static RedisSentinel CreateGCloudSentinel()
        {
            var sentinel = new RedisSentinel(GoogleCloudSentinelHosts, masterName: "master")
            {
                IpAddressMap =
                {
                    {"10.240.34.152", "146.148.77.31"},
                    {"10.240.203.193", "130.211.139.141"},
                    {"10.240.209.52", "107.178.218.53"},
                }
            };
            return sentinel;
        }

        [Test]
        public void Can_connect_directly_to_Redis_Instances()
        {
            foreach (var host in GoogleCloudSentinelHosts)
            {
                using (var client = new RedisClient(host, 6379))
                {
                    "{0}:6379".Print(host);
                    client.Info.PrintDump();
                }

                using (var sentinel = new RedisClient(host, 26379))
                {
                    "{0}:26379".Print(host);
                    sentinel.Info.PrintDump();
                }
            }
        }

        [Test]
        public void Can_connect_to_GoogleCloud_3SentinelSetup()
        {
            var sentinel = CreateGCloudSentinel();

            var redisManager = sentinel.Start();

            using (var client = redisManager.GetClient())
            {
                "{0}:{1}".Print(client.Host, client.Port);

                client.FlushAll();

                client.SetEntry("Sentinel3Setup", "GoogleCloud");

                var result = client.GetEntry("Sentinel3Setup");
                Assert.That(result, Is.EqualTo("GoogleCloud"));
            }

            using (var readOnly = redisManager.GetReadOnlyClient())
            {
                "{0}:{1}".Print(readOnly.Host, readOnly.Port);

                var result = readOnly.GetEntry("Sentinel3Setup");
                Assert.That(result, Is.EqualTo("GoogleCloud"));
            }
        }
    }
}