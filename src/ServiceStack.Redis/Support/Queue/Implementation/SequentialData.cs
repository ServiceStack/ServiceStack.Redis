using System.Collections.Generic;

namespace ServiceStack.Redis.Support.Queue.Implementation
{
    public class SequentialData<T> : ISequentialData<T> where T : class
    {
        private IList<T> workItems;
        private readonly RedisSequentialWorkQueue<T>.DequeueLock dequeueLock;
        private int processedCount;

        public SequentialData(IList<T> workItems, RedisSequentialWorkQueue<T>.DequeueLock dequeueToken)
        {
            this.workItems = workItems;
            this.dequeueLock = dequeueToken;
        }

        public IList<T> WorkItems
        {
            get { return workItems; }
        }

        /// <summary>
        /// pop remaining items that were returned by dequeue, and unlock queue
        /// </summary>
        /// <returns></returns>
        public void PopAndUnlock()
        {
            if (workItems == null || workItems.Count <= 0 || processedCount >= workItems.Count) return;
            dequeueLock.PopAndUnlock(processedCount);
            processedCount = 0;
            workItems = null;
        }

        /// <summary>
        /// indicate that an item has been processed by the caller
        /// </summary>
        public void DoneProcessedWorkItem()
        {
            if (processedCount >= workItems.Count) return;
            dequeueLock.DoneProcessedWorkItem();
            processedCount++;
        }

    }
}