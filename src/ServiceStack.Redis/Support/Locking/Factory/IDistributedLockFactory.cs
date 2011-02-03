namespace ServiceStack.Redis.Support.Locking.Factory
{
    /// <summary>
    /// Locking strategy interface
    /// </summary>
    public interface IDistributedLockFactory
    {
        IDistributedLock CreateLock();
    }
}