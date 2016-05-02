using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceStack.Redis
{
    partial class RedisNativeClient
    {
        public void Set(byte[] key, byte[] value, int expirySeconds, long expiryMs = 0)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            if (expirySeconds > 0)
                SendExpectSuccess(Commands.Set, key, value, Commands.Ex, expirySeconds.ToUtf8Bytes());
            else if (expiryMs > 0)
                SendExpectSuccess(Commands.Set, key, value, Commands.Px, expiryMs.ToUtf8Bytes());
            else
                SendExpectSuccess(Commands.Set, key, value);
        }

        public void SetEx(byte[] key, int expireInSeconds, byte[] value)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            value = value ?? new byte[0];

            if (value.Length > OneGb)
                throw new ArgumentException("value exceeds 1G", "value");

            SendExpectSuccess(Commands.SetEx, key, expireInSeconds.ToUtf8Bytes(), value);
        }

        public byte[] Get(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectData(Commands.Get, key);
        }

        public long HSet(byte[] hashId, byte[] key, byte[] value)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HSet, hashId, key, value);
        }

        public byte[] HGet(byte[] hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectData(Commands.HGet, hashId, key);
        }

        public long Del(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Del, key);
        }

        public long HDel(byte[] hashId, byte[] key)
        {
            AssertHashIdAndKey(hashId, key);

            return SendExpectLong(Commands.HDel, hashId, key);
        }



        public bool PExpire(byte[] key, long ttlMs)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.PExpire, key, ttlMs.ToUtf8Bytes()) == Success;
        }

        public bool Expire(byte[] key, int seconds)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            return SendExpectLong(Commands.Expire, key, seconds.ToUtf8Bytes()) == Success;
        }


        private static void AssertHashIdAndKey(byte[] hashId, byte[] key)
        {
            if (hashId == null)
                throw new ArgumentNullException("hashId");
            if (key == null)
                throw new ArgumentNullException("key");
        }
    }
}
