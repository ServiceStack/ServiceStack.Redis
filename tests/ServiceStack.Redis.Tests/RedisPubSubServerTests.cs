using System;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [Ignore, Explicit("Ignore long running tests")]
    [TestFixture]
    public class RedisPubSubServerTests
    {
        private static RedisPubSubServer CreatePubSubServer(
            int intervalSecs = 1, int timeoutSecs = 3)
        {
            var clientsManager = new RedisManagerPool(TestConfig.MasterHosts);
            using (var redis = clientsManager.GetClient())
                redis.FlushAll();

            var pubSub = new RedisPubSubServer(
                clientsManager,
                "topic:test")
                {
                    HeartbeatInterval = TimeSpan.FromSeconds(intervalSecs),
                    HeartbeatTimeout = TimeSpan.FromSeconds(timeoutSecs)
                };

            return pubSub;
        }

        [Test]
        public void Does_send_heartbeat_pulses()
        {
            int pulseCount = 0;
            using (var pubSub = CreatePubSubServer(intervalSecs: 1, timeoutSecs: 3))
            {
                pubSub.OnHeartbeatReceived = () => "pulse #{0}".Print(++pulseCount);
                pubSub.Start();

                Thread.Sleep(3100);

                Assert.That(pulseCount, Is.GreaterThan(2));
            }
        }

        [Test]
        public void Does_restart_when_Heartbeat_Timeout_exceeded()
        {
            //This auto restarts 2 times before letting connection to stay alive

            int pulseCount = 0;
            int startCount = 0;
            int stopCount = 0;

            using (var pubSub = CreatePubSubServer(intervalSecs: 1, timeoutSecs: 3))
            {
                pubSub.OnStart = () => "start #{0}".Print(++startCount);
                pubSub.OnStop = () => "stop #{0}".Print(++stopCount);
                pubSub.OnHeartbeatReceived = () => "pulse #{0}".Print(++pulseCount);

                //pause longer than heartbeat timeout so autoreconnects
                pubSub.OnControlCommand = op =>
                {
                    if (op == "PULSE" && stopCount < 2)
                        Thread.Sleep(4000);
                };

                pubSub.Start();

                Thread.Sleep(30 * 1000);

                Assert.That(pulseCount, Is.GreaterThan(3));
                Assert.That(startCount, Is.EqualTo(3));
                Assert.That(stopCount, Is.EqualTo(2));
            }
        }
    }
}