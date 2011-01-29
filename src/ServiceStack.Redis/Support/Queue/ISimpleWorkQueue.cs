using System;
using System.Collections.Generic;


namespace ServiceStack.Redis.Support.Queue
{
	public interface ISimpleWorkQueue<T> : IDisposable where T : class
	{
		void Enqueue(T workItem);

		IList<T> Dequeue(int maxBatchSize);
	}
}