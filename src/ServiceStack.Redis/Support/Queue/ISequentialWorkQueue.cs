using System;
using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue
{
    public interface ISequentialWorkQueue<T> : IDisposable where T : class
    {
        void Enqueue(string workItemId, T workItem);
        KeyValuePair<string, IList<T>> Dequeue(int maxBatchSize);
        void PostDequeue(string workItemId);
    }
}