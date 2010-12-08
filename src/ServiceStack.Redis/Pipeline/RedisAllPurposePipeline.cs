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

            if (redisClient.CurrentPipeline != null)
				throw new InvalidOperationException("A pipeline is already in use");

			redisClient.CurrentPipeline = this;
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
        /// <summary>
        /// reset send buffer and remove pipeline reference from client
        /// </summary>
		public void Dispose()
		{
            RedisClient.ResetSendBuffer();
		    RedisClient.CurrentPipeline = null;
		}
	}
}