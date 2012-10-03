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
		internal RedisTypedPipeline(RedisTypedClient<T> redisClient)
			: base(redisClient)
		{
		    Init();
		}

        protected virtual void Init()
        {
             if (RedisClient.Transaction != null)
                throw new InvalidOperationException("A transaction is already in use");

			if (RedisClient.Pipeline != null)
				throw new InvalidOperationException("A pipeline is already in use");

			RedisClient.Pipeline = this;

        }
		public void Flush()
		{
            try
            {


                // flush send buffers
                RedisClient.FlushSendBuffer();

                //receive expected results
                foreach (var queuedCommand in QueuedCommands)
                {
                    queuedCommand.ProcessResult();
                }

            }
            finally
            {
                ClosePipeline();
                RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }
		}
        protected void Execute()
        {
            foreach (var queuedCommand in QueuedCommands)
            {
                var cmd = queuedCommand as QueuedRedisTypedCommand<T>;
                if (cmd != null)
                    cmd.Execute(RedisClient);
            }
        }

	    public bool Replay()
	    {
	        RedisClient.Pipeline = this;
	        Execute();
            Flush();
	        return true;
	    }

	    protected void ClosePipeline()
		{
			RedisClient.ResetSendBuffer();
			RedisClient.Pipeline = null;
		}

		public void Dispose()
		{
			ClosePipeline();
		}
	}
}