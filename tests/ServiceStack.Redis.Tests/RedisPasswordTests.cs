using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisPasswordTests
    {

        [Explicit("Integration")]
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

        [Test]
        public void Passwords_are_not_leaked_in_exception_messages()
        {            
            const string password = "yesterdayspassword";

            Assert.Throws<ServiceStack.Redis.RedisResponseException>(() => {
                try
                {
                    var factory = new PooledRedisClientManager(password + "@" + TestConfig.SingleHost); // redis will throw when using password and it's not configured
                    using (var redis = factory.GetClient())
                    {
                        redis.SetEntry("Foo", "Bar");
                    }
                }
                catch (RedisResponseException ex)
                {
                    Assert.That(ex.Message, Is.Not.StringContaining(password));
                    throw;
                }
            },
	    "Expected an exception after Redis AUTH command; try using a password that doesn't match.");
        }
    }
}