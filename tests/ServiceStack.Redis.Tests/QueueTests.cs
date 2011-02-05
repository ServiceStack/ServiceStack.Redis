using System;
using System.Collections.Generic;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Redis.Support.Queue.Implementation;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class QueueTests
    {
    
        [Test]
        public void TestSequentialWorkQueue()
        {
            using (var queue = new RedisSequentialWorkQueue<string>(10,10,"127.0.0.1",6379,1))
            {
                const int numMessages = 6;
                var messages0 = new List<string>();
                var messages1 = new List<string>();
                var patients = new string[2];
                patients[0] = "patient0";
                patients[1] = "patient1";

                for (int k = 0; k < 2; ++k)
                {

                    for (int i = 0; i < numMessages; ++i)
                    {
                        messages0.Add(String.Format("{0}_message{1}", patients[0], i));
                        queue.Enqueue(patients[0], messages0[i]);
                        messages1.Add(String.Format("{0}_message{1}", patients[1], i));
                        queue.Enqueue(patients[1], messages1[i]);
                    }

                    bool defer = k == 1;

                    var batch = queue.Dequeue(numMessages/2, defer);
                    // check that half of patient[0] messages are returned
                    for (int i = 0; i < numMessages/2; ++i)
                        Assert.AreEqual(batch.WorkItems[i], messages0[i]);
                    Thread.Sleep(5000);
                    Assert.IsTrue(queue.HarvestZombies());
                    batch.WorkItemIdLock.Unlock();

                    // check that all patient[1] messages are returned
                    batch = queue.Dequeue(2*numMessages, defer);
                    // check that batch size is respected
                    Assert.AreEqual(batch.WorkItems.Count, numMessages);
                    for (int i = 0; i < numMessages; ++i)
                        Assert.AreEqual(batch.WorkItems[i], messages1[i]);
                    batch.WorkItemIdLock.Unlock();


                    // check that there are numMessages/2 messages in the queue
                    batch = queue.Dequeue(numMessages, defer);
                    Assert.AreEqual(batch.WorkItemId, patients[0]);
                    Assert.AreEqual(batch.WorkItems.Count, numMessages/2);
                    batch.WorkItemIdLock.Unlock();



                    // check that there are no more messages in the queue
                    batch = queue.Dequeue(numMessages, defer);
                    Assert.IsNull(batch.WorkItemId);
                    Assert.AreEqual(batch.WorkItems.Count, 0);
                }
            }
        }

        [Test]
        public void TestChronologicalWorkQueue()
        {
            using (var queue = new RedisChronologicalWorkQueue<string>(10, 10, "127.0.0.1", 6379))
            {
                const int numMessages = 6;
                var messages = new List<string>();
                var patients = new List<string>();
                var time = new List<double>();

                for (int i = 0; i < numMessages; ++i)
                {
                    time.Add(i);
                    patients.Add(String.Format("patient{0}",i));
                    messages.Add(String.Format("{0}_message{1}", patients[i], i));
                    queue.Enqueue(patients[i], messages[i],i);
                }

                // dequeue half of the messages
                var batch = queue.Dequeue(0, numMessages, numMessages / 2);
                // check that half of patient[0] messages are returned
                for (int i = 0; i < numMessages / 2; ++i)
                    Assert.AreEqual(batch[i].Value, messages[i]);

                // dequeue the rest of the messages
                batch = queue.Dequeue(0,numMessages,2 * numMessages);
                // check that batch size is respected
                Assert.AreEqual(batch.Count, numMessages/2);
                for (int i = 0; i < numMessages/2; ++i)
                    Assert.AreEqual(batch[i].Value, messages[i + numMessages/2]);

                // check that there are no more messages in the queue
                batch = queue.Dequeue(0,numMessages, numMessages);
                Assert.AreEqual(batch.Count, 0);

            }
        }


        [Test]
        public void TestSimpleWorkQueue()
        {
            using (var queue = new RedisSimpleWorkQueue<string>(10, 10, "127.0.0.1", 6379))
            {
                int numMessages = 6;
                var messages = new string[numMessages];
                for (int i = 0; i < numMessages; ++i)
                {
                    messages[i] = String.Format("message#{0}", i);
                    queue.Enqueue(messages[i]);
                }
                var batch = queue.Dequeue(numMessages*2);
                //test that batch size is respected
                Assert.AreEqual(batch.Count, numMessages);

                // test that messages are returned, in correct order
                for (int i = 0; i < numMessages; ++i)
                    Assert.AreEqual(messages[i], batch[i]); 

                //test that messages were removed from queue
                batch = queue.Dequeue(numMessages * 2);
                Assert.AreEqual(batch.Count, 0);

            }
        }


    }
}