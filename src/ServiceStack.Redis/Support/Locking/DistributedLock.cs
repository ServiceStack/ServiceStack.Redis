using System;

namespace ServiceStack.Redis.Support.Locking
{
    public class DistributedLock : IDistributedLock
	{
        public const int LOCK_NOT_ACQUIRED = 0;
        public const int LOCK_ACQUIRED = 1;
        public const int LOCK_RECOVERED = 2;

        protected string lockKey;
        protected long lockExpire;

        private readonly IRedisClient client;

        public DistributedLock(IRedisClient client)
        {
            this.client = client;
        }

		/// <summary>
		/// acquire distributed, non-reentrant lock on key
		/// </summary>
	    /// <param name="key">global key for this lock</param>
		/// <param name="acquisitionTimeout">timeout for acquiring lock</param>
		/// <param name="lockTimeout">timeout for lock, in seconds (stored as value against lock key) </param>
        public virtual long Lock(string key, int acquisitionTimeout, int lockTimeout)
		{
            lockKey = key;

			const int sleepIfLockSet = 200;
			acquisitionTimeout *= 1000; //convert to ms
			int tryCount = (acquisitionTimeout / sleepIfLockSet) + 1;

			var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
			var newLockExpire = CalculateLockExpire(ts, lockTimeout);

		    var nativeClient = GetClient();
            int wasSet = nativeClient.SetNX(key, BitConverter.GetBytes(newLockExpire));
			int totalTime = 0;
			while (wasSet == 0 && totalTime < acquisitionTimeout)
			{
				int count = 0;
				while (wasSet == 0 && count < tryCount && totalTime < acquisitionTimeout)
				{
					System.Threading.Thread.Sleep(sleepIfLockSet);
					totalTime += sleepIfLockSet;					
					ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
					newLockExpire = CalculateLockExpire(ts, lockTimeout);
                    wasSet = nativeClient.SetNX(key, BitConverter.GetBytes(newLockExpire));
					count++;
				}
				// acquired lock!
                if (wasSet != LOCK_NOT_ACQUIRED) break;

				// handle possibliity of crashed client still holding the lock
                using (var pipe = nativeClient.CreatePipeline())
				{
				    long lockValue=0;
					pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
					pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockValue = (x != null) ? BitConverter.ToInt64(x,0) : 0);
					pipe.Flush();

					// if lock value is 0 (key is empty), or expired, then we can try to acquire it
                    ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
					if (lockValue < ts.TotalSeconds)
					{
						ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
						newLockExpire = CalculateLockExpire(ts, lockTimeout);
						using (var trans = client.CreateTransaction())
						{
							var expire = newLockExpire;
							trans.QueueCommand(r => ((RedisNativeClient)r).Set(key, BitConverter.GetBytes(expire)));
							if (trans.Commit())
								wasSet = LOCK_RECOVERED; //recovered lock!
						}
					}
					else
					{
                        nativeClient.UnWatch();
					}
				}
                if (wasSet != LOCK_NOT_ACQUIRED) break;
				System.Threading.Thread.Sleep(sleepIfLockSet);
				totalTime += sleepIfLockSet;
			}
	        if (wasSet != LOCK_NOT_ACQUIRED)
                lockExpire = newLockExpire;
		    return wasSet;

		}

		/// <summary>
		/// unlock key
		/// </summary>
		public virtual bool Unlock()
		{
			if (lockExpire <= 0)
				return false;
		    long lockVal = 0;
            using (var pipe = client.CreatePipeline())
            {
               
                pipe.QueueCommand(r => ((RedisNativeClient) r).Watch(lockKey));
                pipe.QueueCommand(r => ((RedisNativeClient) r).Get(lockKey),
                                  x => lockVal = (x != null) ? BitConverter.ToInt64(x, 0) : 0);
                pipe.Flush();
            }

		    if (lockVal != lockExpire)
		    {
		       ((RedisNativeClient)client).UnWatch();
		        return false;
		    }

            using (var trans = client.CreateTransaction())
            {
                trans.QueueCommand(r => ((RedisNativeClient)r).Del(lockKey));
                return trans.Commit();
            }

		}


        /// <summary>
        /// 
        /// </summary>
        /// <param name="ts"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        private static long CalculateLockExpire(TimeSpan ts, int timeout)
        {
            return (long)(ts.TotalSeconds + timeout + 1.5);
        }

        protected RedisNativeClient GetClient()
        {
            return (RedisNativeClient)client;
        }

	}
}