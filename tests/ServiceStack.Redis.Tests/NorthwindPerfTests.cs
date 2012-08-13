using System;
using System.Diagnostics;
using Northwind.Common.DataModel;
using NUnit.Framework;
using ServiceStack.DataAccess;

namespace ServiceStack.Redis.Tests
{
	[TestFixture]
	public class NorthwindPerfTests
	{
		[Test][Ignore("Hangs")]
		public void Load_Northwind_database_with_redis()
		{
			NorthwindData.LoadData(false);
			GC.Collect();

			var stopWatch = new Stopwatch();
			stopWatch.Start();

			using (var client = new RedisClient(TestConfig.SingleHost))
			{
				LoadNorthwindData(client);
			}

			Debug.WriteLine("stopWatch.ElapsedMilliseconds: " + stopWatch.ElapsedMilliseconds);
		}

		private static void LoadNorthwindData(IBasicPersistenceProvider persistenceProvider)
		{
			persistenceProvider.StoreAll(NorthwindData.Categories);
			persistenceProvider.StoreAll(NorthwindData.Customers);
			persistenceProvider.StoreAll(NorthwindData.Employees);
			persistenceProvider.StoreAll(NorthwindData.Shippers);
			persistenceProvider.StoreAll(NorthwindData.Orders);
			persistenceProvider.StoreAll(NorthwindData.Products);
			persistenceProvider.StoreAll(NorthwindData.OrderDetails);
			persistenceProvider.StoreAll(NorthwindData.CustomerCustomerDemos);
			persistenceProvider.StoreAll(NorthwindData.Regions);
			persistenceProvider.StoreAll(NorthwindData.Territories);
			persistenceProvider.StoreAll(NorthwindData.EmployeeTerritories);
		}
	}

}