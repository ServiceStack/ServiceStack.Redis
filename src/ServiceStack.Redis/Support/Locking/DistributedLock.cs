using System;

namespace ServiceStack.Redis.Support.Locking
{
    public class DistributedLock : IDistributedLock
	{

        private string lockKey;
        private long lockExpire;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="ts"></param>
		/// <param name="timeout"></param>
		/// <returns></returns>
		private static long CalculateLockExpire(TimeSpan ts, int timeout)
		{
			return (long)(ts.TotalSeconds + timeout + 1 + 0.5);
		}

		/// <summary>
		/// acquire distributed, non-reentrant lock on key
		/// </summary>
		/// <param name="client"></param>
		/// <param name="key">global key for this lock</param>
		/// <param name="acquisitionTimeout">timeout for acquiring lock</param>
		/// <param name="lockTimeout">timeout for lock, in seconds (stored as value against lock key) </param>
		public bool Lock(IRedisClient client, string key, int acquisitionTimeout, int lockTimeout)
		{
            lockKey = key;

			const int sleepIfLockSet = 200;
			acquisitionTimeout *= 1000; //convert to ms
			int tryCount = (acquisitionTimeout / sleepIfLockSet) + 1;

			var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
			var newLockExpire = CalculateLockExpire(ts, lockTimeout);

		    var nativeClient = client as RedisNativeClient;
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
				if (wasSet != 0) break;

				// handle possibliity of crashed client still holding the lock
				using (var pipe = client.CreatePipeline())
				{
				    long lockValue=0;
					pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
					pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockValue = (x != null) ? BitConverter.ToInt64(x,0) : 0);
					pipe.Flush();

					// if lock value is 0 (key is empty), or expired, then we can try to acquire it
					if (lockValue < ts.TotalSeconds)
					{
						ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
						newLockExpire = CalculateLockExpire(ts, lockTimeout);
						using (var trans = client.CreateTransaction())
						{
							var expire = newLockExpire;
							trans.QueueCommand(r => ((RedisNativeClient)r).Set(key, BitConverter.GetBytes(expire)));
							if (trans.Commit())
								wasSet = 1; //acquire lock!
						}
					}
					else
					{
                        nativeClient.UnWatch();
					}
				}
				if (wasSet == 1) break;
				System.Threading.Thread.Sleep(sleepIfLockSet);
				totalTime += sleepIfLockSet;
			}
		    var rc = (wasSet == 1);
            if (rc)
                lockExpire = newLockExpire;
		    return (wasSet == 1);

		}
		/// <summary>
		/// unlock key
		/// </summary>
		/// <param name="client"></param>
		public bool Unlock(IRedisClient client)
		{
			if (lockExpire <= 0)
				return false;
			var rc = false;
			using (var pipe = client.CreatePipeline())
			{
			    long lockVal = 0;
				pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(lockKey));
				pipe.QueueCommand(r => ((RedisNativeClient)r).Get(lockKey), x => lockVal = (x != null) ? BitConverter.ToInt64(x,0) : 0);
				pipe.Flush();

                if (lockVal == lockExpire)
                {
                    using (var trans = client.CreateTransaction())
                    {
                        trans.QueueCommand(r => ((RedisNativeClient)r).Del(lockKey));
                        if (trans.Commit())
                            rc = true;
                    }
                }
                else
                {
                  ((RedisNativeClient)client).UnWatch();
                }
			}
			return rc;
		}

	}
}