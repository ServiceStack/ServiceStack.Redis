using System.Threading;
using System;

namespace ServiceStack.Redis.Utilities.Locking
{
    public class WriteLock : IDisposable
    {
        private readonly ReaderWriterLockSlim _lockObject;

        /// <summary>
        /// This class manages a write lock for a local readers/writer lock, 
        /// using the Resource Acquisition Is Initialization pattern
        /// </summary>
        /// <param name="lockObject"></param>
        public WriteLock(ReaderWriterLockSlim lockObject)
        {
            this._lockObject = lockObject;
            lockObject.EnterWriteLock();
        }

        /// <summary>
        /// RAII disposal
        /// </summary>
        public void Dispose()
        {
            _lockObject.ExitWriteLock();
        }
    }
}