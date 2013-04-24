using System;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class ConnectionStringTests
    {
        [Test]
        public void Can_Parse_String_With_All_Properties_Set()
        {
            var connectionString = new ConnectionString("Host=redis01.foo.com;Port=1337;Password=bar;Db=5");
            Assert.That(connectionString.Host, Is.EqualTo("redis01.foo.com"));
            Assert.That(connectionString.Port, Is.EqualTo(1337));
            Assert.That(connectionString.Password, Is.EqualTo("bar"));
            Assert.That(connectionString.Db, Is.EqualTo(5));
        }

        [Test]
        public void Can_Use_Default_Value_For_Host_When_Missing()
        {
            var connectionString = new ConnectionString("Port=1337;Password=bar;Db=5");
            Assert.That(connectionString.Host, Is.EqualTo(RedisClient.DefaultHost));
        }

        [Test]
        public void Can_Use_Default_Value_For_Port_When_Missing()
        {
            var connectionString = new ConnectionString("Host=redis01.foo.com;Password=bar;Db=5");
            Assert.That(connectionString.Port, Is.EqualTo(RedisClient.DefaultPort));
        }

        [Test]
        public void Can_Use_Default_Value_For_Password_When_Missing()
        {
            var connectionString = new ConnectionString("Host=redis01.foo.com;Port=1337;Db=5");
            Assert.That(connectionString.Password, Is.Null);
        }

        [Test]
        public void Can_Use_Default_Value_For_Db_When_Missing()
        {
            var connectionString = new ConnectionString("Host=redis01.foo.com;Password=bar;Port=1337");
            Assert.That(connectionString.Db, Is.EqualTo(RedisClient.DefaultDb));
        }

        [Test]
        public void Can_Use_Default_Values_For_All_Values_When_ConnectionString_Is_Empty()
        {
            var connectionString = new ConnectionString(string.Empty);
            Assert.That(connectionString.Host, Is.EqualTo(RedisClient.DefaultHost));
            Assert.That(connectionString.Port, Is.EqualTo(RedisClient.DefaultPort));
            Assert.That(connectionString.Password, Is.Null);
            Assert.That(connectionString.Db, Is.EqualTo(RedisClient.DefaultDb));
        }

        [Test]
        public void ArgumentNullException_Is_Thrown_When_ConnectionString_Is_Empty()
        {
            Assert.Throws<ArgumentNullException>(() => new ConnectionString(null));
        }
    }
}