using System;

namespace ServiceStack.Redis.Utilities.Locking
{
    /// <summary>
    /// distributed lock class that follows the Resource Allocation Is Initialization pattern
    /// </summary>
    public class DisposableDistributedLock : IDisposable
    {
        private readonly double lockValue;
        private readonly RedisClient client;
        private readonly string key;
        private DistributedLock myLock = new DistributedLock();

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(RedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            this.client = client;
            key = globalLockKey;
            lockValue = myLock.Lock(client, globalLockKey, acquisitionTimeout, lockTimeout);
        }
        /// <summary>
        /// retrieve lock value
        /// </summary>
        public double LockValue
        {
            get{ return lockValue;}
        }

        /// <summary>
        /// unlock
        /// </summary>
        public void Dispose()
        {
            myLock.Unlock(client, key, lockValue);
        }
    }
}
