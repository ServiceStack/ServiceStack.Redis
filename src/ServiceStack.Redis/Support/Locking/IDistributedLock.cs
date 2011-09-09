namespace ServiceStack.Redis.Support.Locking
{
    /// <summary>
    /// Distributed lock interface
    /// </summary>
	public interface IDistributedLock
	{
	    long Lock(string key, int acquisitionTimeout, int lockTimeout, out long lockExpire, IRedisClient client);
	    bool Unlock(long lockExpire, IRedisClient client);
	}
}