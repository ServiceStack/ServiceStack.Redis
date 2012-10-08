using System.Collections.Generic;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests
{
	[TestFixture, Category("Integration")]
	public class RedisClientEvalTests
		: RedisClientTestsBase
	{
		public override void OnBeforeEachTest()
		{
			base.OnBeforeEachTest();
		}

		[Test]
		public void Can_Eval_int()
		{
			var intVal = Redis.GetEvalInt("return 3141591", 0);
			Assert.That(intVal, Is.EqualTo(3141591));
		}

		[Test]
		public void Can_Eval_int_with_args()
		{
			var intVal = Redis.GetEvalInt("return 3141591", 0, "20", "30", "40");
			Assert.That(intVal, Is.EqualTo(3141591));
		}

		[Test]
		public void Can_Eval_int2()
		{
			var intVal = Redis.GetEvalInt("return 3141592", 0);
			Assert.That(intVal, Is.EqualTo(3141592));
		}

		[Test]
		public void Can_Eval_string()
		{
			var strVal = Redis.GetEvalStr(@"return 'abc'", 0);
			Assert.That(strVal, Is.EqualTo("abc"));
		}

		[Test]
		public void Can_Eval_string_with_args()
		{
			var strVal = Redis.GetEvalStr(@"return 'abc'", 0, "at", "dot", "com");
			Assert.That(strVal, Is.EqualTo("abc"));
		}

		[Test]
		public void Can_Eval_multidata_with_args()
		{
			var strVals = Redis.GetEvalMultiData(@"return {'a','b','c'}", 0, "at", "dot", "com");
			Assert.That(strVals, Is.EquivalentTo(new List<string> { "a", "b", "c" }));
		}

	}
}