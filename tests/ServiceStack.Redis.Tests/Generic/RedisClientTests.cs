using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Northwind.Common.DataModel;
using NUnit.Framework;
using ServiceStack.Common.Extensions;
using ServiceStack.Common.Utils;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests.Generic
{
	[TestFixture, Category("Integration")]
    public class RedisClientTests : RedisClientTestsBase
	{
		[TestFixtureSetUp]
		public void TestFixture()
		{
			NorthwindData.LoadData(false);
		}

        public override void OnBeforeEachTest()
        {
            base.OnBeforeEachTest();
            Redis.NamespacePrefix = "GenericRedisClientTests";
        }

		[Test]
		public void Can_GetTypeIdsSet()
		{
			using (var typedClient = Redis.GetTypedClient<OrderDetail>())
			{
				typedClient.StoreAll(NorthwindData.OrderDetails);

				Assert.That(typedClient.TypeIdsSet.Count, Is.EqualTo(NorthwindData.OrderDetails.Count));
			}
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

			var redis = Redis.GetTypedClient<byte[]>();

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
		public void Can_StoreAll_RedisClient()
		{
			var sp = Stopwatch.StartNew();
			Redis.StoreAll(NorthwindData.OrderDetails);

            var orderDetails = Redis.GetAll<OrderDetail>();
			AssertUnorderedListsAreEqual(orderDetails, NorthwindData.OrderDetails);

			Debug.WriteLine(String.Format("\nWrote {0:#,#} in {1:#,#}ms: {2:#,#.##}: items/ms",
				NorthwindData.OrderDetails.Count, sp.ElapsedMilliseconds,
				NorthwindData.OrderDetails.Count / (double)sp.ElapsedMilliseconds));
		}

		[Test]
		public void Can_StoreAll_RedisTypedClient()
		{
			var sp = Stopwatch.StartNew();
            using (var typedClient = Redis.GetTypedClient<OrderDetail>())
			{
				typedClient.StoreAll(NorthwindData.OrderDetails);

				var orderDetails = typedClient.GetAll();
				AssertUnorderedListsAreEqual(orderDetails, NorthwindData.OrderDetails);
			}
			Debug.WriteLine(String.Format("\nWrote {0:#,#} in {1:#,#}ms: {2:#,#.##}: items/ms",
				NorthwindData.OrderDetails.Count, sp.ElapsedMilliseconds,
				NorthwindData.OrderDetails.Count / (double)sp.ElapsedMilliseconds));
		}
        
        [Test]
        public void Can_SetBit_And_GetBit()
        {
            const string key = "BitKey";
            const int offset = 100;
            Redis.SetBit(key, offset, 1);
            Assert.AreEqual(1, Redis.GetBit(key,offset));
        }

		[Test, Explicit]
		public void Can_StoreAll_and_GetAll_from_Northwind()
		{
			var totalRecords
				= NorthwindData.Categories.Count
				  + NorthwindData.Customers.Count
				  + NorthwindData.Employees.Count
				  + NorthwindData.Shippers.Count
				  + NorthwindData.Orders.Count
				  + NorthwindData.OrderDetails.Count
				  + NorthwindData.CustomerCustomerDemos.Count
				  + NorthwindData.Regions.Count
				  + NorthwindData.Territories.Count
				  + NorthwindData.EmployeeTerritories.Count;

			var before = DateTime.Now;

            Redis.StoreAll(NorthwindData.Categories);
            Redis.StoreAll(NorthwindData.Customers);
            Redis.StoreAll(NorthwindData.Employees);
            Redis.StoreAll(NorthwindData.Shippers);
            Redis.StoreAll(NorthwindData.Orders);
            Redis.StoreAll(NorthwindData.Products);
            Redis.StoreAll(NorthwindData.OrderDetails);
            Redis.StoreAll(NorthwindData.CustomerCustomerDemos);
            Redis.StoreAll(NorthwindData.Regions);
            Redis.StoreAll(NorthwindData.Territories);
            Redis.StoreAll(NorthwindData.EmployeeTerritories);

			Debug.WriteLine(String.Format("Took {0}ms to store the entire Northwind database ({1} records)",
				(DateTime.Now - before).TotalMilliseconds, totalRecords));


			before = DateTime.Now;

            var categories = Redis.GetAll<Category>();
            var customers = Redis.GetAll<Customer>();
            var employees = Redis.GetAll<Employee>();
            var shippers = Redis.GetAll<Shipper>();
            var orders = Redis.GetAll<Order>();
            var products = Redis.GetAll<Product>();
            var orderDetails = Redis.GetAll<OrderDetail>();
            var customerCustomerDemos = Redis.GetAll<CustomerCustomerDemo>();
            var regions = Redis.GetAll<Region>();
            var territories = Redis.GetAll<Territory>();
            var employeeTerritories = Redis.GetAll<EmployeeTerritory>();

			Debug.WriteLine(String.Format("Took {0}ms to get the entire Northwind database ({1} records)",
				(DateTime.Now - before).TotalMilliseconds, totalRecords));


			AssertUnorderedListsAreEqual(categories, NorthwindData.Categories);
			AssertUnorderedListsAreEqual(customers, NorthwindData.Customers);
			AssertUnorderedListsAreEqual(employees, NorthwindData.Employees);
			AssertUnorderedListsAreEqual(shippers, NorthwindData.Shippers);
			AssertUnorderedListsAreEqual(orders, NorthwindData.Orders);
			AssertUnorderedListsAreEqual(products, NorthwindData.Products);
			AssertUnorderedListsAreEqual(orderDetails, NorthwindData.OrderDetails);
			AssertUnorderedListsAreEqual(customerCustomerDemos, NorthwindData.CustomerCustomerDemos);
			AssertUnorderedListsAreEqual(regions, NorthwindData.Regions);
			AssertUnorderedListsAreEqual(territories, NorthwindData.Territories);
			AssertUnorderedListsAreEqual(employeeTerritories, NorthwindData.EmployeeTerritories);
		}

        [Test]
        public void Can_Store_And_Get_Entities_As_Hashes()
        {
            var entity = NorthwindData.Customers[0];
            Redis.StoreAsHash(entity);
            var fromDb = Redis.GetFromHash<Northwind.Common.DataModel.Customer>(entity.Id);

            Assert.AreEqual(entity.Address, fromDb.Address);
            Assert.AreEqual(entity.CompanyName,fromDb.CompanyName);
            Assert.AreEqual(entity.Region,fromDb.Region);
        }

        private class ComplexShipper : Shipper
        {
            public ComplexShipper()
            {
                SomeIds = new List<long>();
                Addresses = new Dictionary<string, string>();
            }
            public IList<long> SomeIds { get; set; }
            public IDictionary<string, string> Addresses { get; set; }
        }

        [Test, Ignore("Dictionary serialized differently")]
        public void Can_Store_Complex_Entity_As_Hash()
        {
            var entity = new ComplexShipper()
            {
                CompanyName = "Test Company",
                Phone = "0123456789",
                SomeIds = new List<long>() { 123, 456, 789 },
                Addresses =
                    new Dictionary<string, string>()
                        {
                            { "Home", "1 Some Street, some town" },
                            { "Work", "2 Office Street, City" }
                        }
            };

            entity.Id = (int)(Redis.As<ComplexShipper>().GetNextSequence());
            Redis.As<ComplexShipper>().StoreAsHash(entity);

            var fromDb = Redis.As<ComplexShipper>().GetFromHash(entity.Id);
            Assert.AreEqual(entity.CompanyName, fromDb.CompanyName);
            Assert.AreEqual(entity.Phone,fromDb.Phone);
            Assert.AreEqual(entity.SomeIds, fromDb.SomeIds);
            Assert.AreEqual(entity.Addresses, fromDb.Addresses);
            var addressesSerialized = JsonSerializer.SerializeToString(entity.Addresses);
            Assert.AreEqual(addressesSerialized, Redis.GetValueFromHash(entity.CreateUrn(), "Addresses"));
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
