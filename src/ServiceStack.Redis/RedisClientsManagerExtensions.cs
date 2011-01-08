using System;
using System.Collections.Generic;
using ServiceStack.Redis.Generic;

namespace ServiceStack.Redis
{
	/// <summary>
	/// Useful wrapper IRedisClientsManager to cut down the boiler plat of most IRedisClient access
	/// </summary>
	public static class RedisClientsManagerExtensions
	{
		public static void Exec(this IRedisClientsManager redisManager, Action<IRedisClient> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				lambda(redis);
			}
		}

		public static string Exec(this IRedisClientsManager redisManager, Func<IRedisClient, string> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				return lambda(redis);
			}
		}

		public static long Exec(this IRedisClientsManager redisManager, Func<IRedisClient, long> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				return lambda(redis);
			}
		}

		public static int Exec(this IRedisClientsManager redisManager, Func<IRedisClient, int> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				return lambda(redis);
			}
		}

		public static double Exec(this IRedisClientsManager redisManager, Func<IRedisClient, double> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				return lambda(redis);
			}
		}

		public static bool Exec(this IRedisClientsManager redisManager, Func<IRedisClient, bool> lambda)
		{
			using (var redis = redisManager.GetClient())
			{
				return lambda(redis);
			}
		}

		public static void ExecTrans(this IRedisClientsManager redisManager, Action<IRedisTransaction> lambda)
		{
			using (var redis = redisManager.GetClient())
			using (var trans = redis.CreateTransaction())
			{
				lambda(trans);

				trans.Commit();
			}
		}

		public static void ExecAs<T>(this IRedisClientsManager redisManager, Action<IRedisTypedClient<T>> lambda)
		{
			using (var redis = redisManager.GetClient())
			using (var typedRedis = redis.GetTypedClient<T>())
			{
				lambda(typedRedis);
			}
		}

		public static T ExecAs<T>(this IRedisClientsManager redisManager, Func<IRedisTypedClient<T>, T> lambda)
		{
			using (var redis = redisManager.GetClient())
			using (var typedRedis = redis.GetTypedClient<T>())
			{
				return lambda(typedRedis);
			}
		}

		public static IList<T> ExecAs<T>(this IRedisClientsManager redisManager, Func<IRedisTypedClient<T>, IList<T>> lambda)
		{
			using (var redis = redisManager.GetClient())
			using (var typedRedis = redis.GetTypedClient<T>())
			{
				return lambda(typedRedis);
			}
		}

		public static List<T> ExecAs<T>(this IRedisClientsManager redisManager, Func<IRedisTypedClient<T>, List<T>> lambda)
		{
			using (var redis = redisManager.GetClient())
			using (var typedRedis = redis.GetTypedClient<T>())
			{
				return lambda(typedRedis);
			}
		}
	}

}