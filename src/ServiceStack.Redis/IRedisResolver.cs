using System.Collections.Generic;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Resolver strategy for resolving hosts and creating clients
    /// </summary>
    public interface IRedisResolver
    {
        int ReadWriteHostsCount { get; }
        int ReadOnlyHostsCount { get; }

        void ResetMasters(IEnumerable<string> hosts);
        void ResetSlaves(IEnumerable<string> hosts);

        RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite);

        RedisEndpoint GetReadWriteHost(int desiredIndex);
        RedisEndpoint GetReadOnlyHost(int desiredIndex);
    }

    public interface IHasRedisResolver
    {
        IRedisResolver RedisResolver { get; set; }
    }
}