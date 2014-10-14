using System;
using System.Linq;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    [TestFixture]
    public class CustomCommandTests
    {
        [Test]
        public void Can_send_custom_commands()
        {
            var redis = RedisClient.New();
            redis.FlushAll();

            RedisText ret;

            ret = redis.Custom("SET", "foo", 1);
            Assert.That(ret.Text, Is.EqualTo("OK"));
            ret = redis.Custom(Commands.Set, "bar", "b");

            ret = redis.Custom("GET", "foo");
            Assert.That(ret.Text, Is.EqualTo("1"));
            ret = redis.Custom(Commands.Get, "bar");
            Assert.That(ret.Text, Is.EqualTo("b"));

            ret = redis.Custom(Commands.Keys, "*");
            var keys = ret.GetResults();
            Assert.That(keys, Is.EquivalentTo(new[] { "foo", "bar" }));

            ret = redis.Custom("MGET", "foo", "bar");
            var values = ret.GetResults();
            Assert.That(values, Is.EquivalentTo(new[] { "1", "b" }));

            Enum.GetNames(typeof(DayOfWeek)).ToList()
                .ForEach(x => redis.Custom("RPUSH", "DaysOfWeek", x));

            ret = redis.Custom("LRANGE", "DaysOfWeek", 1, -2);

            var weekDays = ret.GetResults();
            Assert.That(weekDays, Is.EquivalentTo(
                new[] { "Monday", "Tuesday", "Wednesday", "Thursday", "Friday" }));

            ret.PrintDump();
        }

    }
}