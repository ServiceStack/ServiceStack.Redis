using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
