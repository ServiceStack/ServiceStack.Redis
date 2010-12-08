using System;

namespace ServiceStack.Redis
{
    /// <summary>
    /// base pipeline interface, shared by typed and non-typed pipelines
    /// </summary>
    public interface IRedisPipelineBase : IDisposable, IRedisQueueCompletableOperation
    {
        void Flush();
    }
}
