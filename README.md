[Join the ServiceStack Google+ group](https://plus.google.com/u/0/communities/112445368900682590445) or
follow [@servicestack](http://twitter.com/servicestack) for updates.

# C#/.NET Client for Redis

## New Managed Pub/Sub Server 

The Pub/Sub engine powering 
[Redis ServerEvents](https://github.com/ServiceStack/ServiceStack/wiki/Redis-Server-Events) and 
[Redis MQ](https://github.com/ServiceStack/ServiceStack/wiki/Messaging-and-Redis) has been extracted 
and encapsulated it into a re-usable class that can be used independently for handling messages 
published to specific [Redis Pub/Sub](http://redis.io/commands#pubsub) channels. 

`RedisPubSubServer` processes messages in a managed background thread that **automatically reconnects** 
when the redis-server connection fails and works like an independent background Service that can be 
stopped and started on command. 

The public API is captured in the 
[IRedisPubSubServer](https://github.com/ServiceStack/ServiceStack/blob/master/src/ServiceStack.Interfaces/Redis/IRedisPubSubServer.cs) interface:

```csharp
public interface IRedisPubSubServer : IDisposable
{
    IRedisClientsManager ClientsManager { get; }
    // What Channels it's subscribed to
    string[] Channels { get; }

    // Run once on initial StartUp
    Action OnInit { get; set; }
    // Called each time a new Connection is Started
    Action OnStart { get; set; }
    // Invoked when Connection is broken or Stopped
    Action OnStop { get; set; }
    // Invoked after Dispose()
    Action OnDispose { get; set; }

    // Fired when each message is received
    Action<string, string> OnMessage { get; set; }
    // Fired after successfully subscribing to the specified channels
    Action<string> OnUnSubscribe { get; set; }
    // Called when an exception occurs 
    Action<Exception> OnError { get; set; }
    // Called before attempting to Failover to a new redis master
    Action<IRedisPubSubServer> OnFailover { get; set; }

    int? KeepAliveRetryAfterMs { get; set; }
    // The Current Time for RedisServer
    DateTime CurrentServerTime { get; }

    // Current Status: Starting, Started, Stopping, Stopped, Disposed
    string GetStatus();
    // Different life-cycle stats
    string GetStatsDescription();
    
    // Subscribe to specified Channels and listening for new messages
    IRedisPubSubServer Start();
    // Close active Connection and stop running background thread
    void Stop();
    // Stop than Start
    void Restart();
}
```
### Usage 

To use `RedisPubSubServer`, initialize it with the channels you want to subscribe to and assign handlers 
for each of the events you want to handle. At a minimum you'll want to handle `OnMessage`:

```csharp
var clientsManager = new PooledRedisClientManager();
var redisPubSub = new RedisPubSubServer(clientsManager, "channel-1", "channel-2") {
        OnMessage = (channel, msg) => "Received '{0}' from '{1}'".Print(msg, channel)
    }.Start();
```

Calling `Start()` after it's initialized will get it to start listening and processing any messages 
published to the subscribed channels.

### New Lex Operations

The new [ZRANGEBYLEX](http://redis.io/commands/zrangebylex) sorted set operations allowing you to query a sorted set lexically have been added. 
A good showcase for this is available on [autocomplete.redis.io](http://autocomplete.redis.io/).

These new operations are available as a 1:1 mapping with redis-server on `IRedisNativeClient`:

```csharp
public interface IRedisNativeClient
{
    ...
    byte[][] ZRangeByLex(string setId, string min, string max, int? skip=null, int? take=null);
    long ZLexCount(string setId, string min, string max);
    long ZRemRangeByLex(string setId, string min, string max);
}
```

And the more user-friendly APIs under `IRedisClient`:

```csharp
public interface IRedisClient
{
    ...
    List<string> SearchSortedSet(string setId, string start=null, string end=null);
    long SearchSortedSetCount(string setId, string start=null, string end=null);
    long RemoveRangeFromSortedSetBySearch(string setId, string start=null, string end=null);
}
```

Just like NuGet version matchers, Redis uses `[` char to express inclusiveness and `(` char for exclusiveness.
Since the `IRedisClient` APIs defaults to inclusive searches, these two APIs are the same:

```csharp
Redis.SearchSortedSetCount("zset", "a", "c")
Redis.SearchSortedSetCount("zset", "[a", "[c")
```

Alternatively you can specify one or both bounds to be exclusive by using the `(` prefix, e.g:

```csharp
Redis.SearchSortedSetCount("zset", "a", "(c")
Redis.SearchSortedSetCount("zset", "(a", "(c")
```

More API examples are available in [LexTests.cs](https://github.com/ServiceStack/ServiceStack.Redis/blob/master/tests/ServiceStack.Redis.Tests/LexTests.cs).

### New HyperLog API

The development branch of Redis server (available when v3.0 is released) includes an ingenious algorithm to approximate the unique elements in a set with maximum space and time efficiency. For details about how it works see Redis's creator Salvatore's blog who [explains it in great detail](http://antirez.com/news/75). Essentially it lets you maintain an efficient way to count and merge unique elements in a set without having to store its elements. 
A Simple example of it in action:

```csharp
redis.AddToHyperLog("set1", "a", "b", "c");
redis.AddToHyperLog("set1", "c", "d");
var count = redis.CountHyperLog("set1"); //4

redis.AddToHyperLog("set2", "c", "d", "e", "f");

redis.MergeHyperLogs("mergedset", "set1", "set2");

var mergeCount = redis.CountHyperLog("mergedset"); //6
```

### New Scan APIs Added

Redis v2.8 introduced a beautiful new [SCAN](http://redis.io/commands/scan) operation that provides an optimal strategy for traversing a redis instance entire keyset in managable-size chunks utilizing only a client-side cursor and without introducing any server state. It's a higher performance alternative and should be used instead of [KEYS](http://redis.io/commands/keys) in application code. SCAN and its related operations for traversing members of Sets, Sorted Sets and Hashes are now available in the Redis Client in the following API's:

```csharp
public interface IRedisClient
{
    ...
    IEnumerable<string> ScanAllKeys(string pattern = null, int pageSize = 1000);
    IEnumerable<string> ScanAllSetItems(string setId, string pattern = null, int pageSize = 1000);
    IEnumerable<KeyValuePair<string, double>> ScanAllSortedSetItems(string setId, string pattern = null, int pageSize = 1000);
    IEnumerable<KeyValuePair<string, string>> ScanAllHashEntries(string hashId, string pattern = null, int pageSize = 1000);    
}

//Low-level API
public interface IRedisNativeClient
{
    ...
    ScanResult Scan(ulong cursor, int count = 10, string match = null);
    ScanResult SScan(string setId, ulong cursor, int count = 10, string match = null);
    ScanResult ZScan(string setId, ulong cursor, int count = 10, string match = null);
    ScanResult HScan(string hashId, ulong cursor, int count = 10, string match = null);
}
```

The `IRedisClient` provides a higher-level API that abstracts away the client cursor to expose a lazy Enumerable sequence to provide an optimal way to stream scanned results that integrates nicely with LINQ, e.g:

```csharp
var scanUsers = Redis.ScanAllKeys("urn:User:*");
var sampleUsers = scanUsers.Take(10000).ToList(); //Stop after retrieving 10000 user keys 
```


### New IRedisClient LUA API's

The `IRedisClient` API's for [redis server-side LUA support](http://redis.io/commands/eval) have been re-factored into the more user-friendly API's below:

```csharp
public interface IRedisClient 
{
    //Eval/Lua operations 

    string ExecLuaAsString(string luaBody, params string[] args);
    string ExecLuaAsString(string luaBody, string[] keys, string[] args);
    string ExecLuaShaAsString(string sha1, params string[] args);
    string ExecLuaShaAsString(string sha1, string[] keys, string[] args);
    
    int ExecLuaAsInt(string luaBody, params string[] args);
    int ExecLuaAsInt(string luaBody, string[] keys, string[] args);
    int ExecLuaShaAsInt(string sha1, params string[] args);
    int ExecLuaShaAsInt(string sha1, string[] keys, string[] args);

    List<string> ExecLuaAsList(string luaBody, params string[] args);
    List<string> ExecLuaAsList(string luaBody, string[] keys, string[] args);
    List<string> ExecLuaShaAsList(string sha1, params string[] args);
    List<string> ExecLuaShaAsList(string sha1, string[] keys, string[] args);

    string CalculateSha1(string luaBody);
    
    bool HasLuaScript(string sha1Ref);
    Dictionary<string, bool> WhichLuaScriptsExists(params string[] sha1Refs);
    void RemoveAllLuaScripts();
    void KillRunningLuaScript();
    string LoadLuaScript(string body);
}
```

### Usage Examples

Here's how you can implement a ZPOP in Lua to remove the items with the lowest rank from a sorted set:

```csharp
var luaBody = @"
    local val = redis.call('zrange', KEYS[1], 0, ARGV[1]-1)
    if val then redis.call('zremrangebyrank', KEYS[1], 0, ARGV[1]-1) end
    return val";

var i = 0;
var alphabet = 26.Times(c => ((char)('A' + c)).ToString());
alphabet.ForEach(x => Redis.AddItemToSortedSet("zalphabet", x, i++));

//Remove the letters with the lowest rank from the sorted set 'zalphabet'
var letters = Redis.ExecLuaAsList(luaBody, keys: new[] { "zalphabet" }, args: new[] { "3" });
letters.PrintDump(); //[A, B, C]
```

And how to implement ZREVPOP to remove items with the highest rank from a sorted set:

```csharp
var luaBody = @"
    local val = redis.call('zrange', KEYS[1], -ARGV[1], -1)
    if val then redis.call('zremrangebyrank', KEYS[1], -ARGV[1], -1) end
    return val";

var i = 0;
var alphabet = 26.Times(c => ((char)('A' + c)).ToString());
alphabet.ForEach(x => Redis.AddItemToSortedSet("zalphabet", x, i++));

//Remove the letters with the highest rank from the sorted set 'zalphabet'
List<string> letters = Redis.ExecLuaAsList(luaBody, keys: new[] { "zalphabet" }, args: new[] { "3" });

letters.PrintDump(); //[X, Y, Z]
```

### Other examples

Returning an int:

```csharp
int intVal = Redis.ExecLuaAsInt("return 123"); //123
int intVal = Redis.ExecLuaAsInt("return ARGV[1] + ARGV[2]", "10", "20"); //30
```

Returning an string:

```csharp
var strVal = Redis.ExecLuaAsString(@"return 'Hello, ' .. ARGV[1] .. '!'", "Redis Lua"); //Hello, Redis Lua!
```

Returning a List of strings:

```csharp
Enum.GetNames(typeof(DayOfWeek)).ToList()
    .ForEach(x => Redis.AddItemToList("DaysOfWeek", x));

var daysOfWeek = Redis.ExecLuaAsList("return redis.call('LRANGE', 'DaysOfWeek', 0, -1)");
daysOfWeek.PrintDump(); //[Sunday, Monday, Tuesday, ...]
```

More examples can be found in the [Redis Eval Lua tests](https://github.com/ServiceStack/ServiceStack.Redis/blob/master/tests/ServiceStack.Redis.Tests/RedisClientEvalTests.cs
)

## Overview

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
  * [IRedisTypedClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisTypedClient) - created with `IRedisClient.As<T>()` - it returns a 'strongly-typed client' that provides a typed-interface for all redis value operations that works against any C#/.NET POCO type.

The class hierachy for the C# Redis clients effectively look like:

    RedisTypedClient (POCO) > RedisClient (string) > RedisNativeClient (raw byte[])

Each client provides a different layer of abstraction:
  
  * The RedisNativeClient exposes raw **byte[]** apis and does no marshalling and passes all values directly to redis.
  * The RedisClient assumes **string** values and simply converts strings to UTF8 bytes before sending to Redis
  * The RedisTypedClient provides a generic interface allowing you to add POCO values. The POCO types are serialized using [.NETs fastest JSON Serializer](http://www.servicestack.net/mythz_blog/?p=344) which is then converted to UTF8 bytes and sent to Redis.

### Redis Client API Overview
[![Redis Client API](http://mono.servicestack.net/img/Redis-annotated-preview.png)](http://mono.servicestack.net/img/Redis-annotated.png)

### Thread-safe client managers
For multi-threaded applications you can choose from our different client connection managers:

  * BasicRedisClientManager - a load-balance (master-write and read-slaves) client manager that returns a new [IRedisClient](https://github.com/ServiceStack/ServiceStack.Redis/wiki/IRedisClient) connection with the defaults specified (faster when accessing a redis-server instance on the same host).
  * PooledRedisClientManager - a load-balanced (master-write and read-slaves) client manager that utilizes a pool of redis client connections (faster when accessing a redis-server instance over the network).

## Install ServiceStack.Redis

    PM> Install-Package ServiceStack.Redis

_Latest v4+ on NuGet is a commercial release with [free quotas](https://servicestack.net/download#free-quotas)._

### [Docs and Downloads for older v3 BSD releases](https://github.com/ServiceStackV3/ServiceStackV3)

## Copying

Since September 2013, ServiceStack source code is available under GNU Affero General Public License/FOSS License Exception, see license.txt in the source. 
Alternative commercial licensing is also available, see https://servicestack.net/pricing for details.

## Contributing

Commits should be made to the **v3-fixes** branch so they can be merged into both **v3** and **master** (v4) release branches. 
Contributors need to approve the [Contributor License Agreement](https://docs.google.com/forms/d/16Op0fmKaqYtxGL4sg7w_g-cXXyCoWjzppgkuqzOeKyk/viewform) before any code will be reviewed, see the [Contributing wiki](https://github.com/ServiceStack/ServiceStack/wiki/Contributing) for more details. 

### Redis Server builds for Windows
  
  * [MS Open Tech - Redis on Windows](https://github.com/MSOpenTech/Redis)
  * [Downloads for Cygwin 32bit Redis Server Windows builds](http://code.google.com/p/servicestack/wiki/RedisWindowsDownload).
  * [Project that lets you run Redis as a Windows Service](https://github.com/rgl/redis)
  * [Another Redis as a Windows Service project, which allows you to run separate service for each Redis instance](https://github.com/kcherenkov/redis-windows-service)
  * [Downloads for MinGW 32bit and 64bit Redis Server Windows builds](http://github.com/dmajkic/redis/downloads)

### Redis Virtual Machines

  * [Run Redis in a Vagrant virtual machine](https://github.com/JasonPunyon/redishobo)

# Getting Started with the C# Redis client

###[C# Redis Client wiki](https://github.com/ServiceStack/ServiceStack.Redis/wiki)
Contains all the examples, tutorials and resources you need to get you up to speed with common operations and the latest features.

[Useful Links on Redis server](https://github.com/ServiceStack/ServiceStack.Redis/wiki/Useful-Redis-Links)

### Specific Examples
  * [Using Transactions in Redis (i.e. MULTI/EXEC/DISCARD)](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisTransactions)
  * [Using Redis's built-in Publish/Subscribe pattern for high performance network notifications](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisPubSub)
  * [Using Redis to create high performance *distributed locks* spannable across multiple app servers](https://github.com/ServiceStack/ServiceStack.Redis/wiki/RedisLocks)

# Simple example using Redis Lists

Below is a simple example to give you a flavour of how easy it is to use some of Redis's advanced data structures - in this case Redis Lists:
_Full source code of this example is [viewable online](https://github.com/ServiceStack/ServiceStack.Redis/blob/master/tests/ServiceStack.Redis.Tests/ShippersExample.cs)_

    using (var redisClient = new RedisClient())
    {
        //Create a 'strongly-typed' API that makes all Redis Value operations to apply against Shippers
        IRedisTypedClient<Shipper> redis = redisClient.As<Shipper>();

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


# Community Resources

  - [Distributed Caching using Redis Server with .NET/C# Client](http://www.codeproject.com/Articles/636730/Distributed-Caching-using-Redis) by [Sem.Shekhovtsov](http://www.codeproject.com/script/Membership/View.aspx?mid=6495187)
  - [Fan Messaging with ServiceStack.Redis](http://cornishdev.wordpress.com/2013/04/04/fan-messaging-with-servicestack-redis/) by [miket](http://stackoverflow.com/users/1804544/miket)
  - [Redis and VB.Net](http://blogs.lessthandot.com/index.php/DataMgmt/DBProgramming/redis-and-vb-net) by [@chrissie1](https://twitter.com/chrissie1)
  - [Using ServiceStack.Redis Part 2: Sets and Hashes](http://michaelsarchet.com/using-servicestack-redis-part-2-sets-and-hashes/) by [@msarchet](http://twitter.com/msarchet)
  - [Using the ServiceStack.Redis Client](http://michaelsarchet.com/using-the-servicestack-redis-client/) by [@msarchet](http://twitter.com/msarchet)
  - [Implementing ServiceStack.Redis.RedisClient (.NET Client for Redis)](http://www.narizwallace.com/2012/10/implementing-servicestack-redis-redisclient-net-client-for-redis/) by [@NarizWallace](https://twitter.com/NarizWallace)
  - [Getting started with Redis in ASP.NET under Windows](http://maxivak.com/getting-started-with-redis-and-asp-net-mvc-under-windows/) by [@maxivak](https://twitter.com/maxivak)
  - [Using Redis on Windows with ServiceStack](http://www.clippersoft.net/using-redis-on-windows-with-servicestack/)
