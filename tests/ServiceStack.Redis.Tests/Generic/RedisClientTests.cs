using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;

namespace ServiceStack.Redis.Tests.Generic
{
	[TestFixture, Category("Integration")]
    public class RedisClientTests : RedisClientTestsBase
	{
		[TestFixtureSetUp]
		public void TestFixture()
		{
		}

        public override void OnBeforeEachTest()
        {
            base.OnBeforeEachTest();
            Redis.NamespacePrefix = "GenericRedisClientTests";
        }
        
		[Test]
		public void Can_Set_and_Get_string()
		{
			const string value = "value";
			Redis.SetEntry("key", value);
			var valueString = Redis.GetValue("key");

			Assert.That(valueString, Is.EqualTo(value));
		}

		[Test]
		public void Can_Set_and_Get_key_with_all_byte_values()
		{
			const string key = "bytesKey";
			
			var value = new byte[256];
			for (var i = 0; i < value.Length; i++)
			{
				value[i] = (byte) i;
			}

			var redis = Redis.As<byte[]>();

			redis.SetEntry(key, value);
			var resultValue = redis.GetValue(key);

			Assert.That(resultValue, Is.EquivalentTo(value));
		}

		public List<T> Sort<T>(IEnumerable<T> list)
		{
			var sortedList = list.ToList();
			sortedList.Sort((x, y) => 
				x.GetId().ToString().CompareTo(y.GetId().ToString()));

			return sortedList;
		}

		public void AssertUnorderedListsAreEqual<T>(IList<T> actualList, IList<T> expectedList)
		{
			Assert.That(actualList, Has.Count.EqualTo(expectedList.Count));

			var actualMap = Sort(actualList.Select(x => x.GetId()));
			var expectedMap = Sort(expectedList.Select(x => x.GetId()));

			Assert.That(actualMap, Is.EquivalentTo(expectedMap));
		}
        
        [Test]
        public void Can_SetBit_And_GetBit_And_BitCount()
        {
            const string key = "BitKey";
            const int offset = 100;
            Redis.SetBit(key, offset, 1);
            Assert.AreEqual(1, Redis.GetBit(key,offset));
            Assert.AreEqual(1, Redis.BitCount(key));
        }
        
		public class Dummy
		{
			public int Id { get; set; }
			public string Name { get; set; }
		}

		[Test]
		public void Can_Delete()
		{
			var dto = new Dummy { Id = 1, Name = "Name" };

            Redis.Store(dto);

            Assert.That(Redis.GetAllItemsFromSet(Redis.NamespacePrefix + "ids:Dummy").ToArray()[0], Is.EqualTo("1"));
            Assert.That(Redis.GetById<Dummy>(1), Is.Not.Null);

            Redis.Delete(dto);

            Assert.That(Redis.GetAllItemsFromSet(Redis.NamespacePrefix + "ids:Dummy").Count, Is.EqualTo(0));
            Assert.That(Redis.GetById<Dummy>(1), Is.Null);
		}

		[Test]
		public void Can_DeleteById()
		{
			var dto = new Dummy { Id = 1, Name = "Name" };
            Redis.Store(dto);

            Assert.That(Redis.GetAllItemsFromSet(Redis.NamespacePrefix + "ids:Dummy").ToArray()[0], Is.EqualTo("1"));
            Assert.That(Redis.GetById<Dummy>(1), Is.Not.Null);

            Redis.DeleteById<Dummy>(dto.Id);

            Assert.That(Redis.GetAllItemsFromSet(Redis.NamespacePrefix + "ids:Dummy").Count, Is.EqualTo(0));
            Assert.That(Redis.GetById<Dummy>(1), Is.Null);
		}

	}
}
