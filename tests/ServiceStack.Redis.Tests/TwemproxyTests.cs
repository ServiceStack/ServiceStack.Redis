using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture, Explicit]
    public class TwemproxyTests
    {
        [Test]
        public void Can_connect_to_twemproxy()
        {
            var redis = new RedisClient("10.0.0.14", 22121)
            {
                //ServerVersionNumber = 2611
            };
            //var redis = new RedisClient("10.0.0.14");
            redis.SetEntry("foo", "bar");
            var foo = redis.GetEntry("foo");

            Assert.That(foo, Is.EqualTo("bar"));
        }
    }
}