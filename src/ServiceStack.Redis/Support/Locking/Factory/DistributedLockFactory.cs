namespace ServiceStack.Redis.Support.Locking.Factory
{
    /// <summary>
    /// Locking strategy interface
    /// </summary>
    public class DistributedLockFactory : IDistributedLockFactory
    {
        public IDistributedLock CreateLock()
        {
            return new DistributedLock();
        }
    }
}