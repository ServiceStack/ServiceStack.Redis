using System;
using System.Diagnostics;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisBenchmarkTests
        : RedisClientTestsBase
    {
        const string Value = "Value";

        [Test]
        public void Performance()
        {
            var sw = Stopwatch.StartNew();
            int total = 500;
            var temp = new byte[1];
            for (int i = 0; i < total; ++i)
            {
                ((RedisNativeClient)Redis).Set("key", temp);
            }
            sw.Stop();
            Console.WriteLine(String.Format("Time for {0} Set(key,value) operations: {1} ms", total, sw.ElapsedMilliseconds));
        }

    }

}
