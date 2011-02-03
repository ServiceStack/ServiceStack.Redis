using System;

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
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(RedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            this.client = client;
            myLock = new DistributedLock();
            myLock.Lock(client, globalLockKey, acquisitionTimeout, lockTimeout);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="myLock"></param>
        public DisposableDistributedLock(RedisClient client, IDistributedLock myLock)
        {
            this.client = client;
            this.myLock = myLock;
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
