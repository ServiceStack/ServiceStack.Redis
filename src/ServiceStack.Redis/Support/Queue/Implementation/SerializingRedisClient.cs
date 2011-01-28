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
        /// <param name="key">global lock key</param>
        /// <param name="setLockValue">value that lock key was set to when it was locked</param>
        public bool Unlock(string key, long setLockValue)
        {
            return myLock.Unlock(this, key, setLockValue);
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
        ///  Deserialize buffer to object
        /// </summary>
        /// <param name="someBytes">byte array to deserialize</param>
        /// <returns></returns>
        public  object Deserialize(byte[] someBytes)
        {
            return serializer.Deserialize(someBytes);
        }

    }
}