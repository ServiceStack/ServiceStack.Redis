using System.Collections;
using System.Collections.Generic;
using ServiceStack.Redis.Support.Locking;

namespace ServiceStack.Redis.Support.Queue.Implementation
{
    public class SerializingRedisClient : RedisClient
    {
        private readonly DistributedLock myLock = new DistributedLock();
        private ISerializer serializer = new ObjectSerializer();
   
        public SerializingRedisClient(string host, int port)
            : base(host, port)
        {
        }
        
        /// <summary>
        /// acquire distributed, non-reentrant lock on key
        /// </summary>
        /// <param name="key">global key for this lock</param>
        /// <param name="acquisitionTimeout">timeout for acquiring lock</param>
        /// <param name="lockTimeout">timeout for lock, in seconds (stored as value against lock key) </param>
        public long Lock(string key, int acquisitionTimeout, int lockTimeout)
        {
            return myLock.Lock(this, key, acquisitionTimeout, lockTimeout);

        }
        /// <summary>
        /// unlock key
        /// </summary>
        /// <param name="setLockValue">value that lock key was set to when it was locked</param>
        public bool Unlock()
        {
            return myLock.Unlock(this);
        }

        /// <summary>
        /// customize the client serializer
        /// </summary>
        public ISerializer Serializer
        {
            set{ serializer = value;}
        }

        /// <summary>
        ///  Serialize object to buffer
        /// </summary>
        /// <param name="value">serializable object</param>
        /// <returns></returns>
        public  byte[] Serialize(object value)
        {
            return serializer.Serialize(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values">array of serializable objects</param>
        /// <returns></returns>
        public List<byte[]> Serialize(object[] values)
        {
            var rc = new List<byte[]>();
            foreach (var value in values)
            {
                var bytes = Serialize(value);
                if (bytes != null)
                    rc.Add(bytes);
            }
            return rc;
        }

        /// <summary>
        ///  Deserialize buffer to object
        /// </summary>
        /// <param name="someBytes">byte array to deserialize</param>
        /// <returns></returns>
        public  object Deserialize(byte[] someBytes)
        {
            return serializer.Deserialize(someBytes);
        }

        /// deserialize an array of byte arrays
        /// </summary>
        /// <param name="byteArray"></param>
        /// <returns></returns>
        public IList Deserialize(byte[][] byteArray)
        {
            IList rc = new ArrayList();
            foreach (var someBytes in byteArray)
            {
                var obj = Deserialize(someBytes);
                if (obj != null)
                    rc.Add(obj);
            }
            return rc;
        }

    }
}