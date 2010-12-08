using System;
using ServiceStack.Redis.Generic;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Pipeline for redis typed client
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class RedisTypedPipeline<T> : RedisTypedCommandQueue<T>, IRedisTypedPipeline<T>
    {
        internal RedisTypedPipeline(RedisTypedClient<T> redisClient): base(redisClient)
        {

            if (redisClient.CurrentPipeline != null)
                throw new InvalidOperationException("A pipeline is already in use");

            redisClient.CurrentPipeline = this;
        }


        public void Flush()
        {
            // flush send buffers
            RedisClient.FlushSendBuffer();

            //receive expected results
            foreach (var queuedCommand in QueuedCommands)
            {
                queuedCommand.ProcessResult();
            }

        }

        public void Dispose()
        {
            RedisClient.ResetSendBuffer();
            RedisClient.CurrentPipeline = null;
        }
    }
}