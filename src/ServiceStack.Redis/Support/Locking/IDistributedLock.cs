namespace ServiceStack.Redis.Support.Locking
{
    /// <summary>
    /// Distributed lock interface
    /// </summary>
	public interface IDistributedLock
	{
	    bool Lock(IRedisClient client, string key, int acquisitionTimeout, int lockTimeout);

	    bool Unlock(IRedisClient client);
	}
}