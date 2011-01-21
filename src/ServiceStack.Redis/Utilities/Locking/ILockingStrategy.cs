using System;

namespace ServiceStack.Redis.Utilities.Locking
{
    public interface ILockingStrategy
    {
        IDisposable ReadLock();

        IDisposable WriteLock();
    }
}
