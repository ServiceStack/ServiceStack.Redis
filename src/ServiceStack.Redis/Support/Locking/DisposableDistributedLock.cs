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
        private readonly long lockExpire;
        private readonly IRedisClient myClient;

        /// <summary>
        /// Lock
        /// </summary>
        /// <param name="client"></param>
        /// <param name="globalLockKey"></param>
        /// <param name="acquisitionTimeout">in seconds</param>
        /// <param name="lockTimeout">in seconds</param>
        public DisposableDistributedLock(IRedisClient client, string globalLockKey, int acquisitionTimeout, int lockTimeout)
        {
            myLock = new DistributedLock();
            myClient = client;
            lockState = myLock.Lock(globalLockKey, acquisitionTimeout, lockTimeout, out lockExpire, myClient);
        }


        public long LockState
        {
            get { return lockState; } 
        }

        public long LockExpire
        {
            get { return lockExpire; }
        }


        /// <summary>
        /// unlock
        /// </summary>
        public void Dispose()
        {
            myLock.Unlock(lockExpire, myClient);
        }
    }
}
