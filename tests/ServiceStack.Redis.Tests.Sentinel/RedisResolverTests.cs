using System;
using System.Collections.Generic;
using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests.Sentinel
{
    [TestFixture]
    public class RedisResolverTests
        : RedisSentinelTestBase
    {
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

        [Test]
        public void RedisResolver_does_reset_when_detects_invalid_master()
        {
            var invalidMaster = new[] { SlaveHosts[0] };
            var invalidSlaves = new[] { MasterHosts[0], SlaveHosts[1] };

            using (var redisManager = new PooledRedisClientManager(invalidMaster, invalidSlaves))
            {
                var resolver = (RedisResolver)redisManager.RedisResolver;

                using (var master = redisManager.GetClient())
                {
                    master.SetValue("KEY", "1");
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                }
                using (var master = redisManager.GetClient())
                {
                    master.Increment("KEY", 1);
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                }

                "Masters:".Print();
                resolver.Masters.PrintDump();
                "Slaves:".Print();
                resolver.Slaves.PrintDump();
            }
        }

        [Test]
        public void PooledRedisClientManager_alternates_hosts()
        {
            using (var redisManager = new PooledRedisClientManager(MasterHosts, SlaveHosts))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(SlaveHosts[i % SlaveHosts.Length]));
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
            using (var redisManager = new RedisManagerPool(MasterHosts))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(MasterHosts[0]));
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
            using (var redisManager = new BasicRedisClientManager(MasterHosts, SlaveHosts))
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.Increment("KEY", 1);
                }

                5.Times(i =>
                {
                    using (var readOnly = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(readOnly.GetHostString(), Is.EqualTo(SlaveHosts[i % SlaveHosts.Length]));
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
                this.ClientFactory = RedisConfig.ClientFactory;
            }

            public Func<RedisEndpoint, RedisClient> ClientFactory { get; set; }

            public int ReadWriteHostsCount { get { return 1; } }
            public int ReadOnlyHostsCount { get { return 1; } }

            public void ResetMasters(IEnumerable<string> hosts) { }
            public void ResetSlaves(IEnumerable<string> hosts) { }

            public RedisClient CreateRedisClient(RedisEndpoint config, bool master)
            {
                NewClientsInitialized++;
                return ClientFactory(config);
            }

            public RedisClient CreateMasterClient(int desiredIndex)
            {
                return CreateRedisClient(master, master: true);
            }

            public RedisClient CreateSlaveClient(int desiredIndex)
            {
                return CreateRedisClient(slave, master: false);
            }
        }

        [Test]
        public void PooledRedisClientManager_can_execute_CustomResolver()
        {
            var resolver = new FixedResolver(MasterHosts[0].ToRedisEndpoint(), SlaveHosts[0].ToRedisEndpoint());
            using (var redisManager = new PooledRedisClientManager("127.0.0.1:8888")
            {
                RedisResolver = resolver
            })
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.Increment("KEY", 1);
                }
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                5.Times(i =>
                {
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(SlaveHosts[0]));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                5.Times(i =>
                {
                    using (var master = redisManager.GetClient())
                    {
                        Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                        Assert.That(master.GetValue("KEY"), Is.EqualTo("2"));
                    }
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(SlaveHosts[0]));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(4));
            }
        }

        [Test]
        public void RedisManagerPool_can_execute_CustomResolver()
        {
            var resolver = new FixedResolver(MasterHosts[0].ToRedisEndpoint(), SlaveHosts[0].ToRedisEndpoint());
            using (var redisManager = new RedisManagerPool("127.0.0.1:8888")
            {
                RedisResolver = resolver
            })
            {
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.SetValue("KEY", "1");
                }
                using (var master = redisManager.GetClient())
                {
                    Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                    master.Increment("KEY", 1);
                }
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                5.Times(i =>
                {
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(MasterHosts[0]));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(1));

                redisManager.FailoverTo("127.0.0.1:9999", "127.0.0.1:9999");

                5.Times(i =>
                {
                    using (var master = redisManager.GetClient())
                    {
                        Assert.That(master.GetHostString(), Is.EqualTo(MasterHosts[0]));
                        Assert.That(master.GetValue("KEY"), Is.EqualTo("2"));
                    }
                    using (var slave = redisManager.GetReadOnlyClient())
                    {
                        Assert.That(slave.GetHostString(), Is.EqualTo(MasterHosts[0]));
                        Assert.That(slave.GetValue("KEY"), Is.EqualTo("2"));
                    }
                });
                Assert.That(resolver.NewClientsInitialized, Is.EqualTo(2));
            }
        }

        private static void InitializeEmptyRedisManagers(IRedisClientsManager redisManager, string[] masters, string[] slaves)
        {
            var hasResolver = (IHasRedisResolver)redisManager;
            hasResolver.RedisResolver.ResetMasters(masters);
            hasResolver.RedisResolver.ResetSlaves(slaves);

            using (var master = redisManager.GetClient())
            {
                Assert.That(master.GetHostString(), Is.EqualTo(masters[0]));
                master.SetValue("KEY", "1");
            }
            using (var slave = redisManager.GetReadOnlyClient())
            {
                Assert.That(slave.GetHostString(), Is.EqualTo(slaves[0]));
                Assert.That(slave.GetValue("KEY"), Is.EqualTo("1"));
            }
        }

        [Test]
        public void Can_initalize_ClientManagers_with_no_hosts()
        {
            InitializeEmptyRedisManagers(new PooledRedisClientManager(), MasterHosts, SlaveHosts);
            InitializeEmptyRedisManagers(new RedisManagerPool(), MasterHosts, MasterHosts);
            InitializeEmptyRedisManagers(new BasicRedisClientManager(), MasterHosts, SlaveHosts);
        }
    }
}