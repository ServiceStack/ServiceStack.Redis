
using System;
using System.Threading.Tasks;
using System.Timers;
using ServiceStack;
using ServiceStack.Redis;
using ServiceStack.Text;

namespace TestRedisConnection
{
    class Program
    {
        public static BasicRedisClientManager Manager { get; set; }
        static void Main(string[] args)
        {
            Manager = new BasicRedisClientManager("localhost");

            var q = new System.Timers.Timer { Interval = 2 };
            q.Elapsed += CheckConnection;
            q.Enabled = true;

            if ("q" == Console.ReadLine())
                return;
        }

        private static void CheckConnection(object sender, ElapsedEventArgs e)
        {
            Task.Factory.StartNew(CheckThisConnection);
        }

        private static void CheckThisConnection()
        {
            try
            {
                "CheckThisConnection()...".Print();
                using (var redisClient = Manager.GetClient())
                {
                    using (var trans = redisClient.CreateTransaction())
                    {
                        trans.QueueCommand(
                                 r => r.SetEntryInHash("Test", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test2", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test3", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test4", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test5", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test6", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test7", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test8", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test9", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test10", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test11", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test12", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test13", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test14", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test15", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test16", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test17", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test18", "Price", "123"));
                        trans.QueueCommand(
                                r => r.SetEntryInHash("Test19", "Price", "123"));
                        trans.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                "ERROR: {0}".Print();
            }
        }
    }
}
