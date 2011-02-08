using System.Collections.Generic;

namespace ServiceStack.Redis.Support.Queue
{
    public interface ISequentialData<T>
	{

        /// <summary>
        /// 
        /// </summary>
         IList<T> WorkItems
         { get;}


        /// <summary>
        /// pop numProcessed items from queue and unlock queue for work item id that dequeued
        /// items are associated with
        /// </summary>
        /// <returns></returns>
	    void PopAndUnlock();

        /// <summary>
        /// A dequeued work item has been processed. When all of the dequeued items have been processed,
        /// all items will be popped from the queue,and the queue unlocked for the work item id that
        /// the dequeued items are associated with
        /// </summary>
	    void DoneProcessedWorkItem();
	}
}