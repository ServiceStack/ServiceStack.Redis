namespace ServiceStack.Redis
{
    /// <summary>
    /// Interface to redis pipeline
    /// </summary>
    public interface IRedisPipeline : IRedisPipelineBase, IRedisQueueableOperation
    {
    }
}
