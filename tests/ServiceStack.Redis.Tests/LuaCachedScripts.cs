using NUnit.Framework;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture]
    public class LuaCachedScripts
    {
        private const string LuaScript = @"
local limit = tonumber(ARGV[2])
local pattern = ARGV[1]
local cursor = 0
local len = 0
local results = {}

repeat
    local r = redis.call('scan', cursor, 'MATCH', pattern, 'COUNT', limit)
    cursor = tonumber(r[1])
    for k,v in ipairs(r[2]) do
        table.insert(results, v)
        len = len + 1
        if len == limit then break end
    end
until cursor == 0 or len == limit

return results
";

        private static void AddTestKeys(RedisClient redis, int count)
        {
            count.Times(i =>
                redis.SetValue("key:" + i, "value:" + i));
        }

        [Test]
        public void Can_call_repeated_scans_in_LUA()
        {
            using (var redis = new RedisClient())
            {
                AddTestKeys(redis, 20);

                var r = redis.ExecLua(LuaScript, "key:*", "10");
                Assert.That(r.Children.Count, Is.EqualTo(10));

                r = redis.ExecLua(LuaScript, "key:*", "40");
                Assert.That(r.Children.Count, Is.EqualTo(20));
            }
        }

        [Test]
        public void Can_call_Cached_Lua()
        {
            using (var redis = new RedisClient())
            {
                AddTestKeys(redis, 20);

                var r = redis.ExecCachedLua(LuaScript, sha1 =>
                    redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));

                r = redis.ExecCachedLua(LuaScript, sha1 =>
                    redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));
            }
        }

        [Test]
        public void Can_call_Cached_Lua_even_after_script_is_flushed()
        {
            using (var redis = new RedisClient())
            {
                var r = redis.ExecCachedLua(LuaScript, sha1 =>
                    redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));

                redis.ScriptFlush();

                r = redis.ExecCachedLua(LuaScript, sha1 =>
                    redis.ExecLuaSha(sha1, "key:*", "10"));
                Assert.That(r.Children.Count, Is.EqualTo(10));
            }
        }
    }
}