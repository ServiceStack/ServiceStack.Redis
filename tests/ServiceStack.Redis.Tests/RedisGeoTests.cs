using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisGeoTests
    {
        private readonly RedisNativeClient redis;

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
            redis.FlushDb();
            redis.GeoAdd("Sicily", 13.361389, 38.115556, "Palermo");
            var results = redis.GeoPos("Sicily", "Palermo");

            Assert.That(results.Count, Is.EqualTo(1));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));
        }

        [Test]
        public void Can_GeoAdd_and_GeoPos_multiple()
        {
            redis.FlushDb();
            redis.GeoAdd("Sicily", 
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var results = redis.GeoPos("Sicily", "Palermo", "Catania");

            Assert.That(results.Count, Is.EqualTo(2));
            Assert.That(results[0].Longitude, Is.EqualTo(13.361389).Within(.1));
            Assert.That(results[0].Latitude, Is.EqualTo(38.115556).Within(.1));
            Assert.That(results[0].Member, Is.EqualTo("Palermo"));

            Assert.That(results[1].Longitude, Is.EqualTo(15.087269).Within(.1));
            Assert.That(results[1].Latitude, Is.EqualTo(37.502669).Within(.1));
            Assert.That(results[1].Member, Is.EqualTo("Catania"));
        }

        [Test]
        public void Can_GeoDist()
        {
            redis.FlushDb();
            redis.GeoAdd("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var distance = redis.GeoDist("Sicily", "Palermo", "Catania");
            Assert.That(distance, Is.EqualTo(166274.15156960039).Within(.1));
        }

        [Test]
        public void Can_GeoHash()
        {
            redis.GeoAdd("Sicily",
                new RedisGeo(13.361389, 38.115556, "Palermo"),
                new RedisGeo(15.087269, 37.502669, "Catania"));

            var hashes = redis.GeoHash("Sicily", "Palermo", "Catania");
            Assert.That(hashes[0], Is.EqualTo("sqc8b49rny0"));
            Assert.That(hashes[1], Is.EqualTo("sqdtr74hyu0"));
        }
    }
}