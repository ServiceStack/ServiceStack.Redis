[Join the new google group](http://groups.google.com/group/servicestack) or
follow [@demisbellot](http://twitter.com/demisbellot) and [@boxerab](http://twitter.com/boxerab)
for twitter updates.

# An Open Source C# Client for Redis

*The Redis client is an independent project and can be used with or without the ServiceStack webservices framework.*

[Redis](http://code.google.com/p/redis/) is one of the fastest and most feature-rich key-value stores to come from the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement.
It is similar to memcached but the dataset is not volatile, and values can either be strings lists, sets, sorted sets or hashes.

[ServiceStack's C# Redis Client](https://github.com/ServiceStack/ServiceStack.Redis) is an Open Source C# Redis client based on [Miguel de Icaza](http://twitter.com/migueldeicaza) previous efforts with [redis-sharp](http://github.com/migueldeicaza/redis-sharp).
 
There are a number of different APIs available which are all a friendly drop-in with your local IOC:
The `ServiceStack.Redis.RedisClient` class below implements the following interfaces:

 * [ICacheClient](https://github.com/ServiceStack/ServiceStack/wiki/Caching) - If you are using Redis solely as a cache, you should bind to the [ServiceStack's common interface](https://github.com/ServiceStack/ServiceStack.Redis/wiki/Caching) as there already are In-Memory an Memcached implementations available in ServiceStack, allowing you to easily switch providers in-future.
 * [IRedisNativeClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisNativeClient) - For those wanting a low-level raw byte access (where you can control your own serialization/deserialization) that map 1:1 with Redis operations of the same name.

For most cases if you require access to Redis-specific functionality you would want to bind to the interface below:

  * [IRedisClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisClient) - Provides a friendlier, more descriptive API that lets you store values as strings (UTF8 encoding).
  * [IRedisTypedClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisTypedClient) - created with `IRedisClient.GetTypedClient<T>()` - it returns a 'strongly-typed client' that provides a typed-interface for all redis value operations that works against any C#/.NET POCO type.

The class hierachy for the C# Redis clients effectively look like:

    RedisTypedClient (POCO) > RedisClient (string) > RedisNativeClient (raw byte[])

Each client is optimized for maximum efficiency and provides layered functionality for maximum developer productivity:
  
  * The RedisNativeClient exposes raw **byte[]** apis and does no marshalling and passes all values directly to redis.
  * The RedisClient assumes **string** values and simply converts strings to UTF8 bytes before sending to Redis
  * The RedisTypedClient provides a generic interface allowing you to add POCO values. The POCO types are serialized using [.NETs fastest JSON Serializer](http://www.servicestack.net/mythz_blog/?p=344) which is then converted to UTF8 bytes and sent to Redis.

At all times you can pick the most optimal Redis Client for your needs so you can achieve maximum efficiency in your applications.

### Redis Client API Overview
[![Redis Client API](http://servicestack.net/img/Redis-annotated-preview.png)](http://servicestack.net/img/Redis-annotated.png)

### Thread-safe client managers
For multi-threaded applications you can choose from our different client connection managers:

  * BasicRedisClientManager - a load-balance (master-write and read-slaves) client manager that returns a new [IRedisClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisClient) connection with the defaults specified (faster when accessing a redis-server instance on the same host).
  * PooledRedisClientManager - a load-balanced (master-write and read-slaves) client manager that utilizes a pool of redis client connections (faster when accessing a redis-server instance over the network).

# Download

You can download the Redis Client in any one of the following ways:

* Packaged by default in [ServiceStack.dll](https://github.com/ServiceStack/ServiceStack/downloads)
* Available to download separately as a stand-alone [ServiceStack.Redis.dll](https://github.com/ServiceStack/ServiceStack.Redis/downloads) 
* As Source Code via Git: `git clone git://github.com/ServiceStack/ServiceStack.Redis.git`
* For those interested in having a GUI admin tool to visualize your Redis data should check out the [Redis Admin UI](http://www.servicestack.net/mythz_blog/?p=381)

[View the release notes](https://github.com/ServiceStack/ServiceStack.Redis/wiki/Redis-Client-Release-Notes).

### Redis Server builds for Windows
  
  * [MS Open Tech - Redis on Windows](https://github.com/MSOpenTech/Redis)
  * [Downloads for Cygwin 32bit Redis Server Windows builds](http://code.google.com/p/servicestack/wiki/RedisWindowsDownload).
  * [Project that lets you run Redis as a Windows Service](https://github.com/rgl/redis)
  * [Another Redis as a Windows Service project, which allows you to run separate service for each Redis instance](https://github.com/kcherenkov/redis-windows-service)
  * [Downloads for MinGW 32bit and 64bit Redis Server Windows builds](http://github.com/dmajkic/redis/downloads)


# Getting Started with the C# Redis client

###[C# Redis Client wiki](https://github.com/ServiceStack/ServiceStack.Redis/wiki)
Contains all the examples, tutorials and resources you need to get you up to speed with common operations and the latest features.

[Useful Links on Redis server](https://github.com/ServiceStack/ServiceStack.Redis/wiki/Useful-Redis-Links)

### Specific Examples
  * [Using Transactions in Redis (i.e. MULTI/EXEC/DISCARD)](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisTransactions)
  * [Using Redis's built-in Publsih/Subscribe pattern for high performance network notifications](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisPubSub)
  * [Using Redis to create high performance *distributed locks* spannable across multiple app servers](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisLocks)

# Simple example using Redis Lists

Below is a simple example to give you a flavour of how easy it is to use some of Redis's advanced data structures - in this case Redis Lists:
_Full source code of this example is [viewable online](https://github.com/ServiceStack/ServiceStack.Redis/blob/master/tests/ServiceStack.Redis.Tests/ShippersExample.cs)_

    using (var redisClient = new RedisClient())
    {
        //Create a 'strongly-typed' API that makes all Redis Value operations to apply against Shippers
        IRedisTypedClient<Shipper> redis = redisClient.GetTypedClient<Shipper>();

        //Redis lists implement IList<T> while Redis sets implement ICollection<T>
        var currentShippers = redis.Lists["urn:shippers:current"];
        var prospectiveShippers = redis.Lists["urn:shippers:prospective"];

        currentShippers.Add(
            new Shipper {
                Id = redis.GetNextSequence(),
                CompanyName = "Trains R Us",
                DateCreated = DateTime.UtcNow,
                ShipperType = ShipperType.Trains,
                UniqueRef = Guid.NewGuid()
            });

        currentShippers.Add(
            new Shipper {
                Id = redis.GetNextSequence(),
                CompanyName = "Planes R Us",
                DateCreated = DateTime.UtcNow,
                ShipperType = ShipperType.Planes,
                UniqueRef = Guid.NewGuid()
            });

        var lameShipper = new Shipper {
            Id = redis.GetNextSequence(),
            CompanyName = "We do everything!",
            DateCreated = DateTime.UtcNow,
            ShipperType = ShipperType.All,
            UniqueRef = Guid.NewGuid()
        };

        currentShippers.Add(lameShipper);

        Dump("ADDED 3 SHIPPERS:", currentShippers);

        currentShippers.Remove(lameShipper);

        Dump("REMOVED 1:", currentShippers);

        prospectiveShippers.Add(
            new Shipper {
                Id = redis.GetNextSequence(),
                CompanyName = "Trucks R Us",
                DateCreated = DateTime.UtcNow,
                ShipperType = ShipperType.Automobiles,
                UniqueRef = Guid.NewGuid()
            });

        Dump("ADDED A PROSPECTIVE SHIPPER:", prospectiveShippers);

        redis.PopAndPushBetweenLists(prospectiveShippers, currentShippers);

        Dump("CURRENT SHIPPERS AFTER POP n' PUSH:", currentShippers);
        Dump("PROSPECTIVE SHIPPERS AFTER POP n' PUSH:", prospectiveShippers);

        var poppedShipper = redis.PopFromList(currentShippers);
        Dump("POPPED a SHIPPER:", poppedShipper);
        Dump("CURRENT SHIPPERS AFTER POP:", currentShippers);

        //reset sequence and delete all lists
        redis.SetSequence(0);
        redis.Remove(currentShippers, prospectiveShippers);
        Dump("DELETING CURRENT AND PROSPECTIVE SHIPPERS:", currentShippers);
    }

    /*
    == EXAMPLE OUTPUT ==

    ADDED 3 SHIPPERS:
    Id:1,CompanyName:Trains R Us,ShipperType:Trains,DateCreated:2010-01-31T11:53:37.7169323Z,UniqueRef:d17c5db0415b44b2ac5da7b6ebd780f5
    Id:2,CompanyName:Planes R Us,ShipperType:Planes,DateCreated:2010-01-31T11:53:37.799937Z,UniqueRef:e02a73191f4b4e7a9c44eef5b5965d06
    Id:3,CompanyName:We do everything!,ShipperType:All,DateCreated:2010-01-31T11:53:37.8009371Z,UniqueRef:d0c249bbbaf84da39fc4afde1b34e332

    REMOVED 1:
    Id:1,CompanyName:Trains R Us,ShipperType:Trains,DateCreated:2010-01-31T11:53:37.7169323Z,UniqueRef:d17c5db0415b44b2ac5da7b6ebd780f5
    Id:2,CompanyName:Planes R Us,ShipperType:Planes,DateCreated:2010-01-31T11:53:37.799937Z,UniqueRef:e02a73191f4b4e7a9c44eef5b5965d06

    ADDED A PROSPECTIVE SHIPPER:
    Id:4,CompanyName:Trucks R Us,ShipperType:Automobiles,DateCreated:2010-01-31T11:53:37.8539401Z,UniqueRef:67d7d4947ebc4b0ba5c4d42f5d903bec

    CURRENT SHIPPERS AFTER POP n' PUSH:
    Id:4,CompanyName:Trucks R Us,ShipperType:Automobiles,DateCreated:2010-01-31T11:53:37.8539401Z,UniqueRef:67d7d4947ebc4b0ba5c4d42f5d903bec
    Id:1,CompanyName:Trains R Us,ShipperType:Trains,DateCreated:2010-01-31T11:53:37.7169323Z,UniqueRef:d17c5db0415b44b2ac5da7b6ebd780f5
    Id:2,CompanyName:Planes R Us,ShipperType:Planes,DateCreated:2010-01-31T11:53:37.799937Z,UniqueRef:e02a73191f4b4e7a9c44eef5b5965d06

    PROSPECTIVE SHIPPERS AFTER POP n' PUSH:

    POPPED a SHIPPER:
    Id:2,CompanyName:Planes R Us,ShipperType:Planes,DateCreated:2010-01-31T11:53:37.799937Z,UniqueRef:e02a73191f4b4e7a9c44eef5b5965d06

    CURRENT SHIPPERS AFTER POP:
    Id:4,CompanyName:Trucks R Us,ShipperType:Automobiles,DateCreated:2010-01-31T11:53:37.8539401Z,UniqueRef:67d7d4947ebc4b0ba5c4d42f5d903bec
    Id:1,CompanyName:Trains R Us,ShipperType:Trains,DateCreated:2010-01-31T11:53:37.7169323Z,UniqueRef:d17c5db0415b44b2ac5da7b6ebd780f5

    DELETING CURRENT AND PROSPECTIVE SHIPPERS:
    */

More examples are available in the [RedisExamples Redis examples page] and in the comprehensive
[test suite](https://github.com/ServiceStack/ServiceStack.Redis/tree/master/tests/ServiceStack.Redis.Tests)


## Speed
One of the best things about Redis is the speed - it is quick.

[This example](https://github.com/ServiceStack/ServiceStack.Redis/blob/master/tests/ServiceStack.Redis.Tests/RedisClientTests.cs)
below stores and gets the entire [Northwind database](http://code.google.com/p/servicestack/source/browse/trunk/Common/Northwind.Benchmarks/Northwind.Common/DataModel/NorthwindData.cs) (3202 records) in less *1.2 secs* - we've never had it so quick!

_(Running inside a VS.NET/R# unit test on a 3 year old iMac)_

    using (var client = new RedisClient())
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
    }
    /*
    == EXAMPLE OUTPUT ==

    Took 1020.0583ms to store the entire Northwind database (3202 records)
    Took 132.0076ms to get the entire Northwind database (3202 records)
    */


Note: The total time taken includes an extra Redis operation for each record to store the id in a Redis set for each
type as well as serializing and de-serializing each record using Service Stack's TypeSerializer.