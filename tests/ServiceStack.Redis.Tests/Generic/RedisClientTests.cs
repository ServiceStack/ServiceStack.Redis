﻿using System;
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
	[TestFixture]
	public class RedisClientTests
	{
		[TestFixtureSetUp]
		public void TestFixture()
		{
			NorthwindData.LoadData(false);
			using (var redis = new RedisClient(TestConfig.SingleHost))
			{
				redis.FlushAll();
			}
		}

		[Test]
		public void Can_GetTypeIdsSet()
		{
			using (var redis = new RedisClient(TestConfig.SingleHost))
			using (var typedClient = redis.GetTypedClient<OrderDetail>())
			{
				typedClient.StoreAll(NorthwindData.OrderDetails);

				Assert.That(typedClient.TypeIdsSet.Count, Is.EqualTo(NorthwindData.OrderDetails.Count));
			}
		}

		[Test]
		public void Can_Set_and_Get_string()
		{
			const string value = "value";
			using (var redis = new RedisClient(TestConfig.SingleHost))
			{
				redis.SetEntry("key", value);
				var valueString = redis.GetValue("key");

				Assert.That(valueString, Is.EqualTo(value));
			}
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

			using (var redisClient = new RedisClient(TestConfig.SingleHost))
			{
				var redis = redisClient.GetTypedClient<byte[]>();

				redis.SetEntry(key, value);
				var resultValue = redis.GetValue(key);

				Assert.That(resultValue, Is.EquivalentTo(value));
			}
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
			using (var client = new RedisClient(TestConfig.SingleHost))
			{
				client.StoreAll(NorthwindData.OrderDetails);

				var orderDetails = client.GetAll<OrderDetail>();
				AssertUnorderedListsAreEqual(orderDetails, NorthwindData.OrderDetails);
			}
			Console.WriteLine("\nWrote {0:#,#} in {1:#,#}ms: {2:#,#.##}: items/ms",
				NorthwindData.OrderDetails.Count, sp.ElapsedMilliseconds,
				NorthwindData.OrderDetails.Count / (double)sp.ElapsedMilliseconds);
		}

		[Test]
		public void Can_StoreAll_RedisTypedClient()
		{
			var sp = Stopwatch.StartNew();
			using (var client = new RedisClient(TestConfig.SingleHost))
			using (var typedClient = client.GetTypedClient<OrderDetail>())
			{
				typedClient.StoreAll(NorthwindData.OrderDetails);

				var orderDetails = typedClient.GetAll();
				AssertUnorderedListsAreEqual(orderDetails, NorthwindData.OrderDetails);
			}
			Console.WriteLine("\nWrote {0:#,#} in {1:#,#}ms: {2:#,#.##}: items/ms",
				NorthwindData.OrderDetails.Count, sp.ElapsedMilliseconds,
				NorthwindData.OrderDetails.Count / (double)sp.ElapsedMilliseconds);
		}

		[Test]
		public void Can_StoreAll_and_GetAll_from_Northwind()
		{
			if (TestConfig.IgnoreLongTests) return;

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

			using (var client = new RedisClient(TestConfig.SingleHost))
			{
				var before = DateTime.Now;

				client.StoreAll(NorthwindData.Categories);
				client.StoreAll(NorthwindData.Customers);
				client.StoreAll(NorthwindData.Employees);
				client.StoreAll(NorthwindData.Shippers);
				client.StoreAll(NorthwindData.Orders);
				client.StoreAll(NorthwindData.Products);
				client.StoreAll(NorthwindData.OrderDetails);
				client.StoreAll(NorthwindData.CustomerCustomerDemos);
				client.StoreAll(NorthwindData.Regions);
				client.StoreAll(NorthwindData.Territories);
				client.StoreAll(NorthwindData.EmployeeTerritories);

				Console.WriteLine("Took {0}ms to store the entire Northwind database ({1} records)",
					(DateTime.Now - before).TotalMilliseconds, totalRecords);


				before = DateTime.Now;

				var categories = client.GetAll<Category>();
				var customers = client.GetAll<Customer>();
				var employees = client.GetAll<Employee>();
				var shippers = client.GetAll<Shipper>();
				var orders = client.GetAll<Order>();
				var products = client.GetAll<Product>();
				var orderDetails = client.GetAll<OrderDetail>();
				var customerCustomerDemos = client.GetAll<CustomerCustomerDemo>();
				var regions = client.GetAll<Region>();
				var territories = client.GetAll<Territory>();
				var employeeTerritories = client.GetAll<EmployeeTerritory>();

				Console.WriteLine("Took {0}ms to get the entire Northwind database ({1} records)",
					(DateTime.Now - before).TotalMilliseconds, totalRecords);


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
		}

        [Test]
        public void Can_Store_And_Get_Entities_As_Hashes()
        {
            var entity = NorthwindData.Customers[0];
            using (var client = new RedisClient(TestConfig.SingleHost))
            {
                client.StoreAsHash(entity);
                var fromDb = client.GetFromHash<Northwind.Common.DataModel.Customer>(entity.Id);

                Assert.AreEqual(fromDb.Address, entity.Address);
                Assert.AreEqual(fromDb.CompanyName, entity.CompanyName);
                Assert.AreEqual(fromDb.Region, entity.Region);
            }
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

        [Test]
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
            using (var client = new RedisClient(TestConfig.SingleHost))
            {
                entity.Id = (int)(client.As<ComplexShipper>().GetNextSequence());
                client.As<ComplexShipper>().StoreAsHash(entity);

                var fromDb = client.As<ComplexShipper>().GetFromHash(entity.Id);
                Assert.AreEqual(entity.CompanyName, fromDb.CompanyName);
                Assert.AreEqual(entity.Phone,fromDb.Phone);
                Assert.AreEqual(entity.SomeIds, fromDb.SomeIds);
                Assert.AreEqual(entity.Addresses, fromDb.Addresses);
                var addressesSerialized = JsonSerializer.SerializeToString(entity.Addresses);
                Assert.AreEqual(client.GetValueFromHash(entity.CreateUrn(), "Addresses"), addressesSerialized);
            }
        }


	}
}
