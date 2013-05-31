using System;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Common;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [Explicit("Simulating error conditions")]
    [TestFixture]
    public class RedisFailoverTests
    {
        [Test]
        public void Can_recover_from_server_terminated_client_connection()
        {
            const int SleepHoldingClientMs = 2;
            const int SleepAfterReleasingClientMs = 0;
            const int loop = 1000;

            var admin = new RedisClient("localhost");
            admin.SetConfig("timeout", "0");
            var timeout = admin.GetConfig("timeout");
            timeout.Print("timeout: {0}");

            int remaining = loop;
            var stopwatch = Stopwatch.StartNew();

            var clientManager = new PooledRedisClientManager(new[] { "localhost" }){ CheckConnected = false };
            loop.Times(i =>
                {
                    ThreadPool.QueueUserWorkItem(x =>
                    {
                        try
                        {
                            using (RedisClient client = (RedisClient)clientManager.GetClient())
                            {
                                client.IncrementValue("key");
                                var val = client.Get<long>("key");
                                "#{0}, isConnected: {1}".Print(val, true); //client.IsSocketConnected()
                                Thread.Sleep(SleepHoldingClientMs);
                            }
                            Thread.Sleep(SleepAfterReleasingClientMs);
                        }
                        catch (Exception ex)
                        {
                            ex.Message.Print();
                        }
                        finally 
                        {
                            remaining--;
                        }
                    });
                });

            while (remaining > 0)
            {
                Thread.Sleep(10);
            }
            "Elapsed time: {0}ms".Print(stopwatch.ElapsedMilliseconds);

            var managerStats = clientManager.GetStats();
            managerStats.PrintDump();
        }
    }
}