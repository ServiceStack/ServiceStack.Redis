namespace ServiceStack.Redis
{
    /// <summary>
    /// Base transaction interface, shared by typed and non-typed transactions
    /// </summary>
    public interface IRedisTransactionBase : IRedisQueueCompletableOperation
    {
        /// <summary>
        /// Queue up Action to read "QUEUED" response from MULTI/EXEC commands
        /// </summary>
        void QueueExpectQueued();
    }
}
