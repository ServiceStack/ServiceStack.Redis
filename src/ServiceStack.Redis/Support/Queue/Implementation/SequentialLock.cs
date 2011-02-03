using System;
using ServiceStack.Redis.Support.Locking;

namespace ServiceStack.Redis.Support.Queue.Implementation
{
    public class SequentialLock : DistributedLock
	{
        private readonly string workItemId;
        private readonly string pendingWorkItemIdQueue;
        private readonly RedisNamespace queueNamespace;
        private ObjectSerializer serializer = new ObjectSerializer();

        public SequentialLock(RedisNamespace queueNamespace, string workItemId, string pendingWorkItemIdQueue)
        {
            this.queueNamespace = queueNamespace;
            this.workItemId = workItemId;
            this.pendingWorkItemIdQueue = pendingWorkItemIdQueue;
        }

    	public override bool Unlock(IRedisClient client)
    	{
            if (lockExpire <= 0)
                return false;
            long lockVal = 0;
            using (var pipe = client.CreatePipeline())
            {

                pipe.QueueCommand(r => ((RedisNativeClient)r).Watch(lockKey));
                pipe.QueueCommand(r => ((RedisNativeClient)r).Get(lockKey),
                                  x => lockVal = (x != null) ? BitConverter.ToInt64(x, 0) : 0);
                pipe.Flush();
            }

            if (lockVal != lockExpire)
            {
                ((RedisNativeClient)client).UnWatch();
                return false;
            }

            var key = queueNamespace.GlobalCacheKey(workItemId);
            var len = ((RedisNativeClient)client).LLen(key);
            //note: if transaction fails, this means that someone else has acquired the lock
            using (var trans = client.CreateTransaction())
            {
                // update pending work queue
                if (len == 0)
                     trans.QueueCommand(r => ((RedisNativeClient)r).ZRem(pendingWorkItemIdQueue, serializer.Serialize(lockKey)) );
                else
                     trans.QueueCommand(r => ((RedisNativeClient)r).ZAdd(pendingWorkItemIdQueue, len, serializer.Serialize(lockKey)) );

                //release the lock
                trans.QueueCommand(r => ((RedisNativeClient)r).Del(lockKey));
                return trans.Commit();
            }
    	}
    
	}
}