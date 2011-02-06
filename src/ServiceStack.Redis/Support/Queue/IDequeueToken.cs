namespace ServiceStack.Redis.Support.Queue
{
    /// <summary>
    /// Token returned to client after dequeue.
    /// </summary>
	public interface IDequeueToken
	{
        /// <summary>
        /// pop numProcessed items from queue and unlock queue for work item id that dequeued
        /// items are associated with
        /// </summary>
        /// <param name="numProcessed"></param>
        /// <returns></returns>
	    bool PopAndUnlock(int numProcessed);

        /// <summary>
        /// A dequeued work item has been processed. When all of the dequeued items have been processed,
        /// all items will be popped from the queue,and the queue unlocked for the work item id that
        /// the dequeued items are associated with
        /// </summary>
	    void DoneProcessedWorkItem();
	}
}