using System;
using ServiceStack.Redis.Support.Locking.Factory;

namespace ServiceStack.Redis.Support.Locking
{
    /// <summary>
    /// distributed lock class that follows the Resource Allocation Is Initialization pattern
    /// </summary>
    public class DisposableDistributedLock : IDisposable
    {
        private readonly IRedisClient client;
        private readonly IDistributedLock myLock;
        private long lockState;

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="clientManager"></param>
        /// <param name="lockFactory"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(IRedisClient client, IDistributedLockFactory lockFactory, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            this.client = client;
            myLock = lockFactory.CreateLock();
            lockState = myLock.Lock(globalLockKey, acquisitionTimeout, lockTimeout);
        }

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(IRedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout) :
            this(client, new DistributedLockFactory(client), globalLockKey, acquisitionTimeout, lockTimeout)
        {
        }

        public long LockState
        {
            get { return lockState; } 
        }

        /// <summary>
        /// unlock
        /// </summary>
        public void Dispose()
        {
            myLock.Unlock();
        }
    }
}
