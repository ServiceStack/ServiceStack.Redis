namespace TestRedisConnection
{
    public class Incr
    {
        public long Id { get; set; }
    }

    public class IncrResponse
    {
        public long Result { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {
            //new LongRunningRedisPubSubServer().Execute("10.0.0.9");
            //new HashStressTest().Execute("127.0.0.1");
            //new HashStressTest().Execute("10.0.0.9");

            new HashCollectionStressTests().Execute("10.0.0.9", noOfThreads: 64);
        }
    }
}
