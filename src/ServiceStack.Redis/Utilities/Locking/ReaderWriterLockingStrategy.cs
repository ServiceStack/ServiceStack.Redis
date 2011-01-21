using System;
using System.Threading;

namespace ServiceStack.Redis.Utilities.Locking
{
    public class ReaderWriterLockingStrategy : ILockingStrategy
    {
        private readonly ReaderWriterLockSlim lockObject = new ReaderWriterLockSlim();


        public IDisposable ReadLock()
        {
            return new ReadLock(lockObject);
        }

        public IDisposable WriteLock()
        {
            return new WriteLock(lockObject);
        }
    }
}
