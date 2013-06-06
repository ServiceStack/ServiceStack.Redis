using System;
using System.Threading;
using ServiceStack.Common;
using ServiceStack.Logging;
using ServiceStack.Logging.Support.Logging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.Text;

namespace TestMqHost
{
    public class Incr
    {
        public int Value { get; set; }
    }

    class Program
    {
        static void Main(string[] args)
        {

            //LogManager.LogFactory = new ConsoleLogFactory();

            var clientManager = new PooledRedisClientManager(new[] { "localhost" })
            {
                PoolTimeout = 1000,
            };

            var mqHost = new RedisMqServer(clientManager, retryCount: 2);

            var sum = 0;
            mqHost.RegisterHandler<Incr>(c =>
            {
                var dto = c.GetBody();
                sum += dto.Value;
                "Received {0}, sum: {1}".Print(dto.Value, sum);
                return null;
            });

            mqHost.Start();

            10.Times(i =>
            {
                ThreadPool.QueueUserWorkItem(x =>
                {
                    using (var client = mqHost.CreateMessageQueueClient())
                    {
                        try
                        {
                            lock (clientManager)
                                "Publish: {0}...".Print(i);
                            client.Publish(new Incr { Value = i });
                        }
                        catch (Exception ex)
                        {
                            lock (clientManager)
                                "Start Publish exception: {0}".Print(ex.Message);
                            clientManager.GetClientPoolActiveStates().PrintDump();
                            clientManager.GetReadOnlyClientPoolActiveStates().PrintDump();
                        }
                        Thread.Sleep(10);
                    }
                });
            });

            ThreadPool.QueueUserWorkItem(_ =>
            {
                using (var client = (RedisClient)clientManager.GetClient())
                {
                    client.SetConfig("timeout", "1");
                    var clientAddrs = client.GetClientList().ConvertAll(x => x["addr"]);
                    lock (clientManager)
                        "Killing clients: {0}...".Print(clientAddrs.Dump());
                    try
                    {
                        clientAddrs.ForEach(client.ClientKill);
                    }
                    catch (Exception ex)
                    {
                        lock (clientManager)
                            "Client exception: {0}".Print(ex.Message);
                    }
                }
            });

            20.Times(i =>
            {
                using (var client = mqHost.CreateMessageQueueClient())
                {
                    try
                    {
                        lock (clientManager)
                            "Publish: {0}...".Print(i);
                        client.Publish(new Incr { Value = i });
                    }
                    catch (Exception ex)
                    {
                        lock (clientManager)
                            "Publish exception: {0}".Print(ex.Message);
                        clientManager.GetClientPoolActiveStates().PrintDump();
                        clientManager.GetReadOnlyClientPoolActiveStates().PrintDump();
                    }
                }

                Thread.Sleep(1000);
            });
        }
    }
}
