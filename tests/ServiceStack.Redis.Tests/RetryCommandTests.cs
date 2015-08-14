using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Timers;
using NUnit.Framework;
using ServiceStack.Logging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RetryCommandTests
    {
        [Test]
        public void Does_retry_failed_commands()
        {
            //LogManager.LogFactory = new ConsoleLogFactory(debugEnabled: true);

            var redisCtrl = new RedisClient(RedisConfig.DefaultHost);
            redisCtrl.FlushAll();
            redisCtrl.SetClient("redisCtrl");

            var redis = new RedisClient(RedisConfig.DefaultHost);
            redis.SetClient("redisRetry");

            var clientInfo = redisCtrl.GetClientsInfo();
            var redisId = clientInfo.First(m => m["name"] == "redisRetry")["id"];
            Assert.That(redisId.Length, Is.GreaterThan(0));

            Assert.That(redis.IncrementValue("retryCounter"), Is.EqualTo(1));

            redis.OnBeforeFlush = () =>
            {
                redisCtrl.KillClients(withId: redisId);
            };

            Assert.That(redis.IncrementValue("retryCounter"), Is.EqualTo(2));
            Assert.That(redis.Get<int>("retryCounter"), Is.EqualTo(2));
        }
    }
}