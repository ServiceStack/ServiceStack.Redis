using ServiceStack.Redis;

namespace ConsoleTests
{
    public class GoogleRedisSentinelFailoverTests : RedisSentinelFailoverTests
    {
        public static string[] SentinelHosts = new[]
        {
            "146.148.77.31",
            "130.211.139.141",
            "107.178.218.53",
        };

        protected override RedisSentinel CreateSentinel()
        {
            var sentinel = new RedisSentinel(SentinelHosts, "master")
            {
                IpAddressMap =
                {
                    {"10.240.34.152", "146.148.77.31"},
                    {"10.240.203.193", "130.211.139.141"},
                    {"10.240.209.52", "107.178.218.53"},
                }
            };
            return sentinel;
        }
    }
}