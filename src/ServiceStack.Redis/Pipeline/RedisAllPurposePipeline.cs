using System;

namespace ServiceStack.Redis
{

	public class RedisAllPurposePipeline: RedisCommandQueue, IRedisPipeline
	{
        /// <summary>
        /// General purpose pipeline
        /// </summary>
        /// <param name="redisClient"></param>
        public RedisAllPurposePipeline(RedisClient redisClient) : base(redisClient)
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
      
        /// <summary>
        /// Flush send buffer, and read responses
        /// </summary>
	    public void Flush()
	    {
	       // flush send buffers
            RedisClient.FlushSendBuffer();

            //receive expected results
           foreach (var queuedCommand in QueuedCommands)
            {
                queuedCommand.ProcessResult();
            }
            ClosePipeline();
            
	    }

        protected void Execute()
        {
            int count = QueuedCommands.Count;
            for (int i = 0; i < count; ++i)
            {
                var cmd = QueuedCommands[0] as QueuedRedisCommand;
                QueuedCommands.RemoveAt(0);
                if (cmd != null)
                {
                    BeginQueuedCommand(cmd);
                    cmd.Execute(RedisClient);
                }
            }
        }

	    public void Replay()
	    {
	        Init();
	        Execute();
	        Flush();
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