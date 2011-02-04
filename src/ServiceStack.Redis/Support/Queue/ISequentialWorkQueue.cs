using System;
using System.Collections.Generic;

namespace ServiceStack.Redis.Support.Queue
{
	public interface ISequentialWorkQueue<T> : IDisposable where T : class
	{
        /// <summary>
        /// Enqueue item in queue corresponding to workItemId identifier
        /// </summary>
        /// <param name="workItemId"></param>
        /// <param name="workItem"></param>
		void Enqueue(string workItemId, T workItem);
		
        /// <summary>
        /// Dequeue up to maxBatchSize items from queue corresponding to workItemId identifier.
        /// Once this method is called, no items for workItemId queue can be dequeued until
        /// <see cref="PostDequeue"/> is called
        /// </summary>
        /// <param name="maxBatchSize"></param>
        /// <returns></returns>
        SequentialDeueueData<T> Dequeue(int maxBatchSize);

    }
}