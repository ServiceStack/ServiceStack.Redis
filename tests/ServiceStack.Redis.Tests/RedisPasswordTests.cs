using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [Explicit("Integration")]
    [TestFixture]
    public class RedisPasswordTests
    {
        [Test]
        public void Can_connect_to_Slaves_and_Masters_with_Password()
        {
            var factory = new PooledRedisClientManager(
                readWriteHosts: new[] { "pass@10.0.0.59:6379" }, 
                readOnlyHosts: new[] { "pass@10.0.0.59:6380" });

            using (var readWrite = factory.GetClient())
            using (var readOnly = factory.GetReadOnlyClient())
            {
                readWrite.SetEntry("Foo", "Bar");
                var value = readOnly.GetEntry("Foo");

                Assert.That(value, Is.EqualTo("Bar"));
            }
        }
    }
}