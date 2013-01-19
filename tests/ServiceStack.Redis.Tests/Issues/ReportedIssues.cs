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
			var redis = new RedisClient(TestConfig.SingleHost);

			redis.AddRangeToSet("testset", storeMembers);

			var members = Redis.GetAllItemsFromSet("testset");
			Assert.That(members, Is.EquivalentTo(storeMembers));
		}

		[Test]
		public void Transaction_fails_if_first_command()
		{
			var redis = new RedisClient(TestConfig.SingleHost);
			using (var trans = redis.CreateTransaction())
			{
				trans.QueueCommand(r => r.IncrementValue("A"));

				trans.Commit();
			}
			Assert.That(redis.GetValue("A"), Is.EqualTo("1"));
		}

        [Test]
        public void Success_callback_fails_for_pipeline_using_GetItemScoreInSortedSet()
        {
            var redis = new RedisClient(TestConfig.SingleHost);
            double score = 0;

            redis.AddItemToSortedSet("testset", "value", 1);

            using (var pipeline = redis.CreatePipeline())
            {
                pipeline.QueueCommand(u => u.GetItemScoreInSortedSet("testset", "value"), x =>
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