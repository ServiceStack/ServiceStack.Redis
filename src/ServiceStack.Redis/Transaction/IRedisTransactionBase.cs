namespace ServiceStack.Redis
{
    /// <summary>
    /// Base transaction interface, shared by typed and non-typed transactions
    /// </summary>
    public interface IRedisTransactionBase : IRedisPipelineBase
    {
    }
}
