using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceStack.Redis.Pipeline
{
    partial class RedisPipelineCommand
    {
        internal async ValueTask<List<long>> ReadAllAsIntsAsync(CancellationToken cancellationToken)
        {
            var results = new List<long>();
            while (cmdCount-- > 0)
            {
                results.Add(await client.ReadLongAsync(cancellationToken).ConfigureAwait(false));
            }

            return results;
        }
        internal async ValueTask<bool> ReadAllAsIntsHaveSuccessAsync(CancellationToken cancellationToken)
        {
            var allResults = await ReadAllAsIntsAsync(cancellationToken).ConfigureAwait(false);
            return allResults.All(x => x == RedisNativeClient.Success);
        }

        internal ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            Flush();
            return default;
        }
    }
}