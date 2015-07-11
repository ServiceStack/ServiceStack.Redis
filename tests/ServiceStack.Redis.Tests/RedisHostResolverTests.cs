using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class RedisHostResolverTests
        : RedisClientTestsBase
    {
        public static int[] RedisServerPorts = new[] { 6380, 6381, 6382 };

        public static string[] RedisMaster = new[] { "127.0.0.1:6380" };
        public static string[] RedisSlaves = new[] { "127.0.0.1:6381", "127.0.0.1:6382" };

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            StartAllRedisServers();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            ShutdownAllRedisServers();
        }

        public static void StartAllRedisServers()
        {
            foreach (var port in RedisServerPorts)
            {
                StartRedisServer(port);
            }
            Thread.Sleep(1000);
        }

        public static void ShutdownAllRedisServers()
        {
            foreach (var port in RedisServerPorts)
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

        private static void StartRedisServer(int port)
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

        [Test]
        public void PooledRedisClientManager_alternates_hosts()
        {
            using (var redisManager = new PooledRedisClientManager(RedisMaster, RedisSlaves))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(RedisSlaves[i % RedisSlaves.Length]));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });

                using (var cahce = redisManager.GetCacheClient())
                {
                    Assert.That(cahce.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        [Test]
        public void RedisManagerPool_alternates_hosts()
        {
            using (var redisManager = new RedisManagerPool(RedisMaster))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(RedisMaster[0]));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });

                using (var cahce = redisManager.GetCacheClient())
                {
                    Assert.That(cahce.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        [Test]
        public void BasicRedisClientManager_alternates_hosts()
        {
            using (var redisManager = new BasicRedisClientManager(RedisMaster, RedisSlaves))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(RedisSlaves[i % RedisSlaves.Length]));
                        Assert.That(readOnly.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });

                using (var cahce = redisManager.GetCacheClient())
                {
                    Assert.That(cahce.Get<string>("KEY"), Is.EqualTo("2"));
                }
            }
        }

        public class FixedResolver : IRedisResolver
        {
            private readonly RedisEndpoint master;
            private readonly RedisEndpoint slave;
            public int NewClientsInitialized = 0;

            public FixedResolver(RedisEndpoint master, RedisEndpoint slave)
            {
                this.master = master;
                this.slave = slave;
            }

            public int ReadWriteHostsCount { get { return 1; } }
            public int ReadOnlyHostsCount { get { return 1; } }

            public void ResetMasters(IEnumerable<string> hosts) {}
            public void ResetSlaves(IEnumerable<string> hosts) {}

            public RedisClient CreateRedisClient(RedisEndpoint config, bool readWrite)
            {
                NewClientsInitialized++;
                return RedisConfig.ClientFactory(config);
            }

            public RedisEndpoint GetReadWriteHost(int desiredIndex)
            {
                return master;
            }

            public RedisEndpoint GetReadOnlyHost(int desiredIndex)
            {
                return slave;
            }
        }

        [Test]
        public void PooledRedisClientManager_can_execute_CustomResolver()
        {
            var resolver = new FixedResolver(RedisMaster[0].ToRedisEndpoint(), RedisSlaves[0].ToRedisEndpoint());
            using (var redisManager = new PooledRedisClientManager("127.0.0.1:8888") {
                RedisResolver = resolver
            })
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.Increment("KEY", 1);
                }
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                5.Times(i =>
                {
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(RedisSlaves[0]));
                        slave.GetValue("KEY").Print();
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                5.Times(i =>
                {
                    using (var master = redisManager.GetClient())
                    {
                        Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                        master.GetValue("KEY").Print();
                    }
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(RedisSlaves[0]));
                        slave.GetValue("KEY").Print();
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(4));
            }
        }

        [Test]
        public void RedisManagerPool_can_execute_CustomResolver()
        {
            var resolver = new FixedResolver(RedisMaster[0].ToRedisEndpoint(), RedisSlaves[0].ToRedisEndpoint());
            using (var redisManager = new RedisManagerPool("127.0.0.1:8888")
            {
                RedisResolver = resolver
            })
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                    master.Increment("KEY", 1);
                }
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                5.Times(i =>
                {
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(RedisMaster[0]));
                        slave.GetValue("KEY").Print();
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                5.Times(i =>
                {
                    using (var master = redisManager.GetClient())
                    {
                        Assert.That(master.GetHostString(), Is.EqualTo(RedisMaster[0]));
                        master.GetValue("KEY").Print();
                    }
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(RedisMaster[0]));
                        slave.GetValue("KEY").Print();
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));
            }
        }

    }
}