using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ServiceStack.Redis.Pipeline;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Redis command that does not get queued
    /// </summary>
    internal class RedisCommand : QueuedRedisOperation
    {
        public Action<IRedisClient> VoidReturnCommand { get; set; }
        public Func<IRedisClient, int> IntReturnCommand { get; set; }
        public Func<IRedisClient, long> LongReturnCommand { get; set; }
        public Func<IRedisClient, bool> BoolReturnCommand { get; set; }
        public Func<IRedisClient, byte[]> BytesReturnCommand { get; set; }
        public Func<IRedisClient, byte[][]> MultiBytesReturnCommand { get; set; }
        public Func<IRedisClient, string> StringReturnCommand { get; set; }
        public Func<IRedisClient, List<string>> MultiStringReturnCommand { get; set; }
        public Func<IRedisClient, double> DoubleReturnCommand { get; set; }

        public override void Execute(IRedisClient client)
        {
            try
            {
                if (VoidReturnCommand != null)
                {
                    VoidReturnCommand(client);

                }
                else if (IntReturnCommand != null)
                {
                    IntReturnCommand(client);

                }
                else if (LongReturnCommand != null)
                {
                    LongReturnCommand(client);

                }
                else if (DoubleReturnCommand != null)
                {
                    DoubleReturnCommand(client);

                }
                else if (BytesReturnCommand != null)
                {
                    BytesReturnCommand(client);

                }
                else if (StringReturnCommand != null)
                {
                    StringReturnCommand(client);

                }
                else if (MultiBytesReturnCommand != null)
                {
                    MultiBytesReturnCommand(client);

                }
                else if (MultiStringReturnCommand != null)
                {
                    MultiStringReturnCommand(client);

                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);
            }
        }
    }
}
