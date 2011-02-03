using System;
using ServiceStack.Redis.Support.Locking.Factory;

namespace ServiceStack.Redis.Support.Locking
{
    /// <summary>
    /// distributed lock class that follows the Resource Allocation Is Initialization pattern
    /// </summary>
    public class DisposableDistributedLock : IDisposable
    {
        private readonly RedisClient client;
        private readonly IDistributedLock myLock;

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="lockFactory"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(RedisClient client, IDistributedLockFactory lockFactory, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            this.client = client;
            myLock = lockFactory.CreateLock();
            myLock.Lock(client, globalLockKey, acquisitionTimeout, lockTimeout);
        }

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(RedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout) :
            this(client, new DistributedLockFactory(), globalLockKey, acquisitionTimeout, lockTimeout)
        {
        }

        /// <summary>
        /// unlock
        /// </summary>
        public void Dispose()
        {
            myLock.Unlock(client);
        }
    }
}
