using NUnit.Framework;
using ServiceStack.Templates;

namespace ServiceStack.Redis.Tests
{
    class RedisTemplateTests
    {
        [Test]
        public void Does_build_connection_string()
        {
            var context = new TemplateContext
            {
                TemplateFilters = { new TemplateRedisFilters() }
            };
            context.Container.AddSingleton<IRedisClientsManager>(() => new RedisManagerPool());
            context.Init();

            Assert.That(context.EvaluateTemplate("{{ redisToConnectionString: host:7000?db=1 }}"),
                Is.EqualTo("host:7000?db=1"));

            Assert.That(context.EvaluateTemplate("{{ { host: 'host' } | redisToConnectionString }}"),
                Is.EqualTo("host:6379?db=0"));

            Assert.That(context.EvaluateTemplate("{{ { port: 7000 } | redisToConnectionString }}"),
                Is.EqualTo("localhost:7000?db=0"));

            Assert.That(context.EvaluateTemplate("{{ { db: 1 } | redisToConnectionString }}"),
                Is.EqualTo("localhost:6379?db=1"));

            Assert.That(context.EvaluateTemplate("{{ { host: 'host', port: 7000, db: 1 } | redisToConnectionString }}"),
                Is.EqualTo("host:7000?db=1"));

            Assert.That(context.EvaluateTemplate("{{ { host: 'host', port: 7000, db: 1, password:'secret' } | redisToConnectionString | raw }}"),
                Is.EqualTo("host:7000?db=1&password=secret"));

            Assert.That(context.EvaluateTemplate("{{ redisConnectionString }}"),
                Is.EqualTo("localhost:6379?db=0"));

            Assert.That(context.EvaluateTemplate("{{ { db: 1 } | redisChangeConnection }}"),
                Is.EqualTo("localhost:6379?db=1"));

            Assert.That(context.EvaluateTemplate("{{ redisConnectionString }}"),
                Is.EqualTo("localhost:6379?db=1"));
        }
    }
}
