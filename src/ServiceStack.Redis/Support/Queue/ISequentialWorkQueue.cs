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
        /// Once this method is called, <see cref="Dequeue"/> or <see cref="Peek"/> will not
        /// return any items for workItemId until the dequeue lock returned is unlocked.
        /// </summary>
        /// <param name="maxBatchSize"></param>
        /// <returns></returns>
        SequentialData<T> Dequeue(int maxBatchSize);

        /// <summary>
        /// Get up to maxBatchSize items from queue corresponding to workItemId identifier.
        /// Items are not removed form the queue. 
        /// Once this method is called, <see cref="Dequeue"/> or <see cref="Peek"/> will not
        /// return any items for workItemId until the dequeue lock returned is unlocked.
        /// </summary>
        /// <param name="maxBatchSize"></param>
        /// <returns></returns>
        SequentialData<T> Peek(int maxBatchSize);


	    bool HarvestZombies();


    }
}