using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisGeoTests
    {
        private RedisNativeClient redis;

        public RedisGeoTests()
        {
            redis = new RedisNativeClient("10.0.0.121");
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            redis.Dispose();
        }

        [Test]
        public void Can_GeoAdd_and_GeoPos()
        {
            redis.GeoAdd("Sicily", 13.361389, 38.115556, "Palermo");
            var results = redis.GeoPos("Sicily", "Palermo");

            Assert.That(results.Count, Is.EqualTo(1));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));
        }
    }
}