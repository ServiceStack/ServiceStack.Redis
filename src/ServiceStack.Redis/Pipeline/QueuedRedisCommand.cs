using System;
using System.Collections.Generic;

namespace ServiceStack.Redis
{
    /// <summary>
    /// A complete redis command, with method to send command, receive response, and run callback on success or failure
    /// </summary>
    internal class QueuedRedisCommand : QueuedRedisOperation
    {

        public Action<IRedisClient> VoidReturnCommand { get; set; }
        public Func<IRedisClient,int> IntReturnCommand { get; set; }
        public Func<IRedisClient,long> LongReturnCommand { get; set; }
        public Func<IRedisClient,bool> BoolReturnCommand { get; set; }
        public Func<IRedisClient,byte[]> BytesReturnCommand { get; set; }
        public Func<IRedisClient,byte[][]> MultiBytesReturnCommand { get; set; }
        public Func<IRedisClient,string> StringReturnCommand { get; set; }
        public Func<IRedisClient,List<string>> MultiStringReturnCommand { get; set; }
        public Func<IRedisClient,double> DoubleReturnCommand { get; set; }

        public void Execute(IRedisClient client)
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
