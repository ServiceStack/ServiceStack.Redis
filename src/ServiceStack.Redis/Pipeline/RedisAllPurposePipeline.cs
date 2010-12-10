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

            if (redisClient.Transaction != null)
                throw new InvalidOperationException("A transaction is already in use");

            if (redisClient.Pipeline != null)
				throw new InvalidOperationException("A pipeline is already in use");

			redisClient.Pipeline = this;
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