using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceStack.Redis
{
    partial class RedisClient
    {
        public bool Set(byte[] key, byte[] value, TimeSpan expiresIn)
        {
            if (AssertServerVersionNumber() >= 2600)
            {
                Exec(r => r.Set(key, value, 0, expiryMs: (long)expiresIn.TotalMilliseconds));
            }
            else
            {
                Exec(r => r.SetEx(key, (int)expiresIn.TotalSeconds, value));
            }

            return true;
        }

        public bool Remove(byte[] key)
        {
            return Del(key) == Success;
        }


        public bool ExpireEntryIn(byte[] key, TimeSpan expireIn)
        {
            if (AssertServerVersionNumber() >= 2600)
            {
                if (expireIn.Milliseconds > 0)
                {
                    return PExpire(key, (long)expireIn.TotalMilliseconds);
                }
            }

            return Expire(key, (int)expireIn.TotalSeconds);
        }
    }
}
