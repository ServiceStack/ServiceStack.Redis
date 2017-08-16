using System;
using System.Linq;
using System.Collections.Generic;
using ServiceStack.Configuration;
using ServiceStack.Templates;

namespace ServiceStack.Redis
{
    public class RedisSearchCursorResult
    {
        public int Cursor { get; set; }
        public List<RedisSearchResult> Results { get; set; }
    }

    public class RedisSearchResult
    {
        public string Id { get; set; }
        public string Type { get; set; }
        public long Ttl { get; set; }
        public long Size { get; set; }
    }

    public class TemplateRedisFilters : TemplateFilter
    {
        public IRedisClientsManager RedisManager { get; set; }
        public IAppSettings AppSettings { get; set; }
        T exec<T>(Func<IRedisClient, T> fn)
        {
            using (var db = RedisManager.GetClient())
            {
                return fn(db);
            }
        }

        static Dictionary<string, int> cmdArgCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) {
            { "SET", 3 }
        };

        List<string> parseCommandString(string cmd)
        {
            var args = new List<string>();
            var lastPos = 0;
            for (var i = 0; i < cmd.Length; i++)
            {
                var c = cmd[i];
                if (c == '{' || c == '[')
                {
                    break; //stop splitting args if value is complex type
                }
                if (c == ' ')
                {
                    var arg = cmd.Substring(lastPos, i - lastPos);
                    args.Add(arg);
                    lastPos = i + 1;

                    //if we've reached the command args count, capture the rest of the body as the last arg
                    if (cmdArgCounts.TryGetValue(args[0], out int argCount) && args.Count == argCount - 1)
                        break;
                }
            }
            args.Add(cmd.Substring(lastPos));
            return args;
        }

        object toObject(RedisText r)
        {
            if (r == null)
                return null;

            if (r.Children != null && r.Children.Count > 0)
            {
                var to = new List<object>();
                for (var i = 0; i < r.Children.Count; i++)
                {
                    var child = r.Children[i];
                    var value = child.Text ?? toObject(child);
                    to.Add(value);
                }
                return to;
            }
            return r.Text;
        }

        public object redisCall(string cmd)
        {
            if (string.IsNullOrEmpty(cmd))
                return null;

            var args = parseCommandString(cmd);
            var objParams = args.Select(x => (object)x).ToArray();
            var redisText = exec(r => r.Custom(objParams));
            var result = toObject(redisText);
            return result;
        }

        public List<RedisSearchResult> redisSearchKeys(TemplateScopeContext scope, string query) => redisSearchKeys(scope, query, null);
        public List<RedisSearchResult> redisSearchKeys(TemplateScopeContext scope, string query, object options)
        {
            var json = redisSearchKeysAsJson(scope, query, options);
            const string noResult = "{\"cursor\":0,\"results\":{}}";
            if (json == noResult)
                return new List<RedisSearchResult>();

            var searchResults = json.FromJson<RedisSearchCursorResult>();
            return searchResults.Results;
        }

        public string redisSearchKeysAsJson(TemplateScopeContext scope, string query, object options)
        {
            if (string.IsNullOrEmpty(query))
                return null;

            var args = scope.AssertOptions(nameof(redisSearchKeys), options);
            var limit = args.TryGetValue("limit", out object value)
                ? value.ConvertTo<int>()
                : scope.GetValue("redis.search.limit") ?? 100;

            const string LuaScript = @"
local limit = tonumber(ARGV[2])
local pattern = ARGV[1]
local cursor = tonumber(ARGV[3])
local len = 0
local keys = {}
repeat
    local r = redis.call('scan', cursor, 'MATCH', pattern, 'COUNT', limit)
    cursor = tonumber(r[1])
    for k,v in ipairs(r[2]) do
        table.insert(keys, v)
        len = len + 1
        if len == limit then break end
    end
until cursor == 0 or len == limit
local cursorAttrs = {['cursor'] = cursor, ['results'] = {}}
if len == 0 then
    return cjson.encode(cursorAttrs)
end

local keyAttrs = {}
for i,key in ipairs(keys) do
    local type = redis.call('type', key)['ok']
    local pttl = redis.call('pttl', key)
    local size = 0
    if type == 'string' then
        size = redis.call('strlen', key)
    elseif type == 'list' then
        size = redis.call('llen', key)
    elseif type == 'set' then
        size = redis.call('scard', key)
    elseif type == 'zset' then
        size = redis.call('zcard', key)
    elseif type == 'hash' then
        size = redis.call('hlen', key)
    end
    local attrs = {['id'] = key, ['type'] = type, ['ttl'] = pttl, ['size'] = size, ['foo'] = 'bar'}
    table.insert(keyAttrs, attrs)    
end
cursorAttrs['results'] = keyAttrs
return cjson.encode(cursorAttrs)";

            var json = exec(r => r.ExecCachedLua(LuaScript, sha1 =>
                r.ExecLuaShaAsString(sha1, query, limit.ToString(), "0")));

            return json;
        }
    }
}
