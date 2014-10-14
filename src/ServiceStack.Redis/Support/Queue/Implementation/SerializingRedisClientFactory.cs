namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// Factory to create SerializingRedisClient objects
    /// </summary>
    public class SerializingRedisClientFactory : IRedisClientFactory
    {
        public static SerializingRedisClientFactory Instance = new SerializingRedisClientFactory();

        public RedisClient CreateRedisClient(RedisEndpoint config)
        {
            return new SerializingRedisClient(config);
        }
    }
}