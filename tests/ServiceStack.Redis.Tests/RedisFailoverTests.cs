using System;
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
            const int loop = 200;

            var admin = new RedisClient("localhost");
            admin.SetConfig("timeout", "1");
            var timeout = admin.GetConfig("timeout");
            timeout.Print("timeout: {0}");

            int remaining = loop;

            var clientManager = new PooledRedisClientManager(new[] { "localhost" });
            loop.Times(i =>
                {
                    ThreadPool.QueueUserWorkItem(x =>
                    {
                        try
                        {
                            using (var client = clientManager.GetClient())
                            {
                                client.IncrementValue("key");
                                var val = client.Get<long>("key");
                                val.ToString().Print();
                                Thread.Sleep(2000);
                            }
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
                Thread.Sleep(100);
            }
            
            var managerStats = clientManager.GetStats();
            managerStats.PrintDump();
        }
    }
}