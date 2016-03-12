using System.Globalization;
using System.Threading;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class CultureInfoTests
        : RedisClientTestsBase
    {
        private CultureInfo previousCulture = CultureInfo.InvariantCulture;

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            previousCulture = Thread.CurrentThread.CurrentCulture;
            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR");
            Thread.CurrentThread.CurrentUICulture = new CultureInfo("fr-FR");
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Thread.CurrentThread.CurrentCulture = previousCulture;
        }

        [Test]
        public void Can_AddItemToSortedSet_in_different_Culture()
        {
            Redis.AddItemToSortedSet("somekey1", "somevalue", 66121.202);
            var score = Redis.GetItemScoreInSortedSet("somekey1", "somevalue");

            Assert.That(score, Is.EqualTo(66121.202));
        }

    }
}