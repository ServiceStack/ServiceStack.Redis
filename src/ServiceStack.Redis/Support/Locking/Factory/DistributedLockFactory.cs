namespace ServiceStack.Redis.Support.Locking.Factory
{
    /// <summary>
    /// Distributed lock factory
    /// </summary>
    public class DistributedLockFactory : IDistributedLockFactory
    {
        private readonly IRedisClient client;
        public DistributedLockFactory(IRedisClient client)
        {
            this.client = client;
        }
        public IDistributedLock CreateLock()
        {
            return new DistributedLock(client);
        }
    }
}