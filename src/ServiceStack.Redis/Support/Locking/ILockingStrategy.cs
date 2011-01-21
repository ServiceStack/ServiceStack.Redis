using System;

namespace ServiceStack.Redis.Support.Locking
{
	public interface ILockingStrategy
	{
		IDisposable ReadLock();

		IDisposable WriteLock();
	}
}