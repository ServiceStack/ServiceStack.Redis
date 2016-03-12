using System.Collections.Generic;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests.Issues
{
    [TestFixture]
    public class ReportedIssues
        : RedisClientTestsBase
    {
        private readonly List<string> storeMembers = new List<string> { "one", "two", "three", "four" };

        [Test]
        public void Add_range_to_set_fails_if_first_command()
        {
            Redis.AddRangeToSet("testset", storeMembers);

            var members = Redis.GetAllItemsFromSet("testset");
            Assert.That(members, Is.EquivalentTo(storeMembers));
        }

        [Test]
        public void Transaction_fails_if_first_command()
        {
            using (var trans = Redis.CreateTransaction())
            {
                trans.QueueCommand(r => r.IncrementValue("A"));

                trans.Commit();
            }
            Assert.That(Redis.GetValue("A"), Is.EqualTo("1"));
        }

        [Test]
        public void Success_callback_fails_for_pipeline_using_GetItemScoreInSortedSet()
        {
            double score = 0;
            Redis.AddItemToSortedSet("testzset", "value", 1);

            using (var pipeline = Redis.CreatePipeline())
            {
                pipeline.QueueCommand(u => u.GetItemScoreInSortedSet("testzset", "value"), x =>
                {
                    //score should be assigned to 1 here
                    score = x;
                });

                pipeline.Flush();
            }

            Assert.That(score, Is.EqualTo(1));
        }
    }
}