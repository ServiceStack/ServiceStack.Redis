using System;
using ServiceStack.Common;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
	public class RedisLock 
		: IDisposable
	{
		private readonly IRedisClient redisClient;
		private readonly string key;

		public RedisLock(IRedisClient redisClient, string key, TimeSpan? timeOut)
		{
			this.redisClient = redisClient;
			this.key = key;

			ExecExtensions.RetryUntilTrue(
				() =>
				    {
                        //This pattern is taken from the redis command for SETNX http://redis.io/commands/setnx
				        
                        //Calculate a unix time for when the lock should expire
                        TimeSpan realSpan = timeOut ?? new TimeSpan(365, 0, 0, 0); //if nothing is passed in the timeout hold for a year
				        DateTime expireTime = DateTime.UtcNow.Add(realSpan);
				        string lockString = (expireTime.ToUnixTimeMs() + 1).ToString();
                        
                        //Try to set the lock, if it does not exist this will succeed and the lock is obtained
				        var nx = redisClient.SetEntryIfNotExists(key, lockString);
                        if (nx)
                            return true;

                        //If we've gotten here then a key for the lock is present. This could be because the lock is
                        //correctly acquired or it could be because a client that had acquired the lock crashed (or didn't release it properly).
                        //Therefore we need to get the value of the lock to see when it should expire
				        string lockExpireString = redisClient.Get<string>(key);
                        long lockExpireTime;
                        if (!long.TryParse(lockExpireString, out lockExpireTime))
                            return false;
                        //If the expire time is greater than the current time then we can't let the lock go yet
                        if (lockExpireTime > DateTime.UtcNow.ToUnixTimeMs())
                            return false;

                        //If the expire time is less than the current time then it wasn't released properly and we can attempt to 
                        //acquire the lock. This is done by setting the lock to our timeout string AND checking to make sure
                        //that what is returned is the old timeout string in order to account for a possible race condition.
                        return redisClient.GetAndSetEntry(key, lockString) == lockExpireString;
				    },
				timeOut
			);
		}

		public void Dispose()
		{
			redisClient.Remove(key);
		}
	}
}