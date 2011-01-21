using System;
using System.Threading;

namespace ServiceStack.Redis.Utilities.Locking
{
    public class ReaderWriterLockingStrategy : ILockingStrategy
    {
        private readonly ReaderWriterLockSlim _lockObject = new ReaderWriterLockSlim();


        public IDisposable ReadLock()
        {
            return new ReadLock(_lockObject);
        }

        public IDisposable WriteLock()
        {
            return new WriteLock(_lockObject);
        }
    }
}
