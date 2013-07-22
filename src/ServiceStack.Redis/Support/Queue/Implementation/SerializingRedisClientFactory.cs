namespace ServiceStack.Redis.Support.Queue.Implementation
{
    /// <summary>
    /// Factory to create SerializingRedisClient objects
    /// </summary>
    public class SerializingRedisClientFactory : IRedisClientFactory
    {
        public static SerializingRedisClientFactory Instance = new SerializingRedisClientFactory();

        public RedisClient CreateRedisClient(string host, int port)
        {
            return new SerializingRedisClient(host, port);
        }

        public RedisClient CreateRedisClient(string host, int port, string password)
        {
            return new SerializingRedisClient(host, port);
        }
    }
}