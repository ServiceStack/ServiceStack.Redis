using System;

namespace ServiceStack.Redis.Support.Locking
{
    /// <summary>
    /// distributed lock class that follows the Resource Allocation Is Initialization pattern
    /// </summary>
    public class DisposableDistributedLock : IDisposable
    {
        private readonly IDistributedLock myLock;
        private readonly long lockState;

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(IRedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            myLock = new DistributedLock(client);
            lockState = myLock.Lock(globalLockKey, acquisitionTimeout, lockTimeout);
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
