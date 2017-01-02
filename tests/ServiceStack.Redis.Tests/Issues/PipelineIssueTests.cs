using System;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests.Issues
{
    [TestFixture]
    public class PipelineIssueTests
        : RedisClientTestsBase
    {
        [Test]
        public void Disposing_Client_Clears_Pipeline()
        {
            var clientMgr = new PooledRedisClientManager(TestConfig.SingleHost);

            using (var client = clientMgr.GetClient())
            {
                client.Set("k1", "v1");
                client.Set("k2", "v2");
                client.Set("k3", "v3");
                
                using (var pipe = client.CreatePipeline())
                {
                    pipe.QueueCommand(c => c.Get<string>("k1"), p => { throw new Exception(); });
                    pipe.QueueCommand(c => c.Get<string>("k2"));

                    try
                    {
                        pipe.Flush();
                    }
                    catch (Exception)
                    {
                        //The exception is expected. Swallow it.
                    }
                }
            }

            using (var client = clientMgr.GetClient())
            {
                Assert.AreEqual("v3", client.Get<string>("k3"));
            }
        }
    }
}