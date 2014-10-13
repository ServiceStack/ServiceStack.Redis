using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class ConfigTests
    {
        [Test]
        [TestCase("host", "{Host:host,Port:6379}")]
        [TestCase("redis://host", "{Host:host,Port:6379}")]
        [TestCase("host:1", "{Host:host,Port:1}")]
        [TestCase("pass@host:1", "{Host:host,Port:1,Password:pass}")]
        [TestCase("nunit:pass@host:1", "{Host:host,Port:1,Client:nunit,Password:pass}")]
        [TestCase("host:1?password=pass&client=nunit", "{Host:host,Port:1,Client:nunit,Password:pass}")]
        [TestCase("host:1?db=2", "{Host:host,Port:1,Db:2}")]
        [TestCase("host?ssl=true", "{Host:host,Port:6380,Ssl:True}")]
        [TestCase("host:1?ssl=true", "{Host:host,Port:1,Ssl:True}")]
        [TestCase("host:1?sendtimeout=2&receiveTimeout=3&idletimeoutsecs=4",
            "{Host:host,Port:1,SendTimeout:2,ReceiveTimeout:3,IdleTimeOutSecs:4}")]
        [TestCase("redis://nunit:pass@host:1?ssl=true&db=2&sendtimeout=3&receiveTimeout=4&idletimeoutsecs=5",
            "{Host:host,Port:1,Ssl:True,Client:nunit,Password:pass,Db:2,SendTimeout:3,ReceiveTimeout:4,IdleTimeOutSecs:5}")]
        public void Does_handle_different_connection_strings_settings(string connString, string expectedJsv)
        {
            var actual = connString.ToRedisEndpoint();
            var expected = expectedJsv.FromJsv<RedisEndpoint>();

            Assert.That(actual, Is.EqualTo(expected), 
                "{0} != {1}".Fmt(actual.ToJsv(), expected.ToJsv()));
        }
    }
}
