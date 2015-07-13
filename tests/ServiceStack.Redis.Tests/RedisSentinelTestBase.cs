using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    public abstract class RedisSentinelTestBase
    {
        public const string MasterName = "mymaster";
        public const string GCloudMasterName = "master";

        public static string[] MasterHosts = new[]
        {
            "127.0.0.1:6380",
        };

        public static string[] SlaveHosts = new[]
        {
            "127.0.0.1:6381",
            "127.0.0.1:6382",
        };

        public static string[] SentinelHosts = new[]
        {
            "127.0.0.1:26380",
            "127.0.0.1:26381",
            "127.0.0.1:26382",
        };

        public static int[] RedisPorts = new[]
        {
            6380,
            6381,
            6382,
        };

        public static int[] SentinelPorts = new[]
        {
            26380,
            26381,
            26382,
        };

        public static string[] GoogleCloudSentinelHosts = new[]
        {
            "146.148.77.31",
            "130.211.139.141",
            "107.178.218.53",
        };

        public static RedisSentinel CreateSentinel()
        {
            var sentinel = new RedisSentinel(SentinelHosts);
            return sentinel;
        }

        public static RedisSentinel CreateGCloudSentinel()
        {
            var sentinel = new RedisSentinel(GoogleCloudSentinelHosts, masterName: "master")
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


        public static void StartRedisServer(int port)
        {
            var pInfo = new ProcessStartInfo
            {
                FileName = new FileInfo(@"..\..\..\..\src\sentinel\redis\redis-server.exe").FullName,
                Arguments = new FileInfo(@"..\..\..\..\src\sentinel\redis-{0}\redis.windows.conf".Fmt(port)).FullName,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            ThreadPool.QueueUserWorkItem(state => Process.Start(pInfo));
        }

        public static void StartRedisSentinel(int port)
        {
            var pInfo = new ProcessStartInfo
            {
                FileName = new FileInfo(@"..\..\..\..\src\sentinel\redis\redis-server.exe").FullName,
                Arguments = new FileInfo(@"..\..\..\..\src\sentinel\redis-{0}\sentinel.conf".Fmt(port)).FullName + " --sentinel",
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            };

            ThreadPool.QueueUserWorkItem(state => Process.Start(pInfo));
        }

        public static void StartAllRedisServers(int waitMs = 1500)
        {
            foreach (var port in RedisPorts)
            {
                StartRedisServer(port);
            }
            if (waitMs > 0)
                Thread.Sleep(waitMs);
        }

        public static void StartAllRedisSentinels(int waitMs = 1500)
        {
            foreach (var port in RedisPorts)
            {
                StartRedisSentinel(port);
            }
            if (waitMs > 0)
                Thread.Sleep(waitMs);
        }

        public static void ShutdownAllRedisServers()
        {
            foreach (var port in RedisPorts)
            {
                try
                {
                    var client = new RedisClient("127.0.0.1", port);
                    client.ShutdownNoSave();
                }
                catch (Exception ex)
                {
                    "Error trying to shutdown {0}".Print(port);
                    ex.Message.Print();
                }
            }
        }

        public static void ShutdownAllRedisSentinels()
        {
            foreach (var port in SentinelPorts)
            {
                try
                {
                    var client = new RedisClient("127.0.0.1", port);
                    client.ShutdownNoSave();
                }
                catch (Exception ex)
                {
                    "Error trying to shutdown {0}".Print(port);
                    ex.Message.Print();
                }
            }
        }

    }
}