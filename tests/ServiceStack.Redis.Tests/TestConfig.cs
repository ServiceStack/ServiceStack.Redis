using ServiceStack.Logging;
using ServiceStack.Support;

namespace ServiceStack.Redis.Tests
{
	public static class TestConfig
	{
		static TestConfig()
		{
			LogManager.LogFactory = new InMemoryLogFactory();
		}

		public const bool IgnoreLongTests = true;

        public const string SingleHost = "localhost";
        public const string SentinelHost = "10.0.0.9";
        public const string MasterName = "mymaster";
        public static readonly string[] MasterHosts = new[] { "localhost" };
        public static readonly string[] SlaveHosts = new[] { "localhost" };

	    public const int RedisPort = 6379;
	    public const int RedisSentinelPort = 26379;

		public static string SingleHostConnectionString
		{
			get
			{
				return SingleHost + ":" + RedisPort;
			}
		}

		public static BasicRedisClientManager BasicClientManger
		{
			get
			{
				return new BasicRedisClientManager(new[] {
					SingleHostConnectionString
				});
			}
		}
	}
}