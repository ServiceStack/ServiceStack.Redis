using System;

namespace ServiceStack.Redis.Support.Locking
{
	public class DistributedLock
	{
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
		public long Lock(RedisClient client, string key, int acquisitionTimeout, int lockTimeout)
		{
			const int sleepIfLockSet = 200;
			acquisitionTimeout *= 1000; //convert to ms
			int tryCount = (acquisitionTimeout / sleepIfLockSet) + 1;

			var ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
			long lockExpire = CalculateLockExpire(ts, lockTimeout);

			/* TODO: using the ObjectSerializer/BinaryFormatter seems pretty inefficient, we should look for an alternative.
			 * It would be good if any distributed locking can be as efficient as possible.
			 * If we only need it for doubles so we don't lose any precision we may want to copy what protobuf-net is doing?
			 * http://code.google.com/p/protobuf-net/source/browse/trunk/protobuf-net/ProtoWriter.cs
			 * i.e. BitConverter.ToInt64(BitConverter.GetBytes(value), 0);
			 */
			int wasSet = client.SetNX(key, BitConverter.GetBytes(lockExpire));
			int totalTime = 0;
			while (wasSet == 0 && totalTime < acquisitionTimeout)
			{
				int count = 0;
				while (wasSet == 0 && count < tryCount && totalTime < acquisitionTimeout)
				{
					System.Threading.Thread.Sleep(sleepIfLockSet);
					totalTime += sleepIfLockSet;					
					ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
					lockExpire = CalculateLockExpire(ts, lockTimeout);
                    wasSet = client.SetNX(key, BitConverter.GetBytes(lockExpire));
					count++;
				}
				// acquired lock!
				if (wasSet != 0) break;

				// handle possibliity of crashed client still holding the lock
				using (var pipe = client.CreatePipeline())
				{
				    long lockVal=0;
					pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
					pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockVal = (x != null) ? BitConverter.ToInt64(x,0) : 0);
					pipe.Flush();

					// if lock value is 0 (key is empty), or expired, then we can try to acquire it
					if (lockVal < ts.TotalSeconds)
					{
						ts = (DateTime.UtcNow - new DateTime(1970, 1, 1, 0, 0, 0));
						lockExpire = CalculateLockExpire(ts, lockTimeout);
						using (var trans = client.CreateTransaction())
						{
							var expire = lockExpire;
							trans.QueueCommand(r => ((RedisNativeClient)r).Set(key, BitConverter.GetBytes(expire)));
							if (trans.Commit())
								wasSet = 1; //acquire lock!
						}
					}
					else
					{
						client.UnWatch();
					}
				}
				if (wasSet == 1) break;
				System.Threading.Thread.Sleep(sleepIfLockSet);
				totalTime += sleepIfLockSet;
			}
			return (wasSet == 1) ? lockExpire : 0;

		}
		/// <summary>
		/// unlock key
		/// </summary>
		/// <param name="client"></param>
		/// <param name="key">global lock key</param>
		/// <param name="setLockValue">value that lock key was set to when it was locked</param>
		public bool Unlock(RedisClient client, string key, long setLockValue)
		{
			if (setLockValue <= 0)
				return false;
			var rc = false;
			using (var pipe = client.CreatePipeline())
			{
			    long lockVal = 0;
				pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(key));
				pipe.QueueCommand(r => ((RedisNativeClient)r).Get(key), x => lockVal = (x != null) ? BitConverter.ToInt64(x,0) : 0);
				pipe.Flush();

                if (lockVal == setLockValue)
                {
                    using (var trans = client.CreateTransaction())
                    {
                        trans.QueueCommand(r => ((RedisNativeClient)r).Del(key));
                        if (trans.Commit())
                            rc = true;
                    }
                }
                else
                {
                  client.UnWatch();
                }
			}
			return rc;
		}

	}
}