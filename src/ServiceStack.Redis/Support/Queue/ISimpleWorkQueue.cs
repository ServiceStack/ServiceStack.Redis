using System;
using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue
{
	public interface ISimpleWorkQueue<T> : IDisposable where T : class
	{
		void Enqueue(T workItem);

        void UnDequeue(IList<T> workItems);

		IList<T> Dequeue(int maxBatchSize);
	}
}