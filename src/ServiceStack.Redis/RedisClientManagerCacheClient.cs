using System;
using System.Collections.Generic;
using ServiceStack.CacheAccess;

namespace ServiceStack.Redis
{
    public class RedisClientManagerCacheClient : ICacheClient
    {
        private readonly ICacheClient redisManager;

        public RedisClientManagerCacheClient(ICacheClient redisManager)
        {
            this.redisManager = redisManager;
        }

        /// <summary>
        /// Ignore dispose on RedisClientsManager, which should be registered as a singleton
        /// </summary>
        public void Dispose() {}

        public bool Remove(string key)
        {
            return redisManager.Remove(key);
        }

        public void RemoveAll(IEnumerable<string> keys)
        {
            redisManager.RemoveAll(keys);
        }

        public T Get<T>(string key)
        {
            return redisManager.Get<T>(key);
        }

        public long Increment(string key, uint amount)
        {
            return redisManager.Increment(key, amount);
        }

        public long Decrement(string key, uint amount)
        {
            return redisManager.Decrement(key, amount);
        }

        public bool Add<T>(string key, T value)
        {
            return redisManager.Add(key, value);
        }

        public bool Set<T>(string key, T value)
        {
            return redisManager.Set(key, value);
        }

        public bool Replace<T>(string key, T value)
        {
            return redisManager.Replace(key, value);
        }

        public bool Add<T>(string key, T value, DateTime expiresAt)
        {
            return redisManager.Add(key, value, expiresAt);
        }

        public bool Set<T>(string key, T value, DateTime expiresAt)
        {
            return redisManager.Set(key, value, expiresAt);
        }

        public bool Replace<T>(string key, T value, DateTime expiresAt)
        {
            return redisManager.Replace(key, value, expiresAt);
        }

        public bool Add<T>(string key, T value, TimeSpan expiresIn)
        {
            return redisManager.Add(key, value, expiresIn);
        }

        public bool Set<T>(string key, T value, TimeSpan expiresIn)
        {
            return redisManager.Set(key, value, expiresIn);
        }

        public bool Replace<T>(string key, T value, TimeSpan expiresIn)
        {
            return redisManager.Replace(key, value, expiresIn);
        }

        public void FlushAll()
        {
            redisManager.FlushAll();
        }

        public IDictionary<string, T> GetAll<T>(IEnumerable<string> keys)
        {
            return redisManager.GetAll( keys);
        }

        public void SetAll<T>(IDictionary<string, T> values)
        {
            redisManager.SetAll(values);
        }
    }
}