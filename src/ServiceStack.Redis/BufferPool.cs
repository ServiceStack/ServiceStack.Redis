using System;
using System.Diagnostics;
using System.Threading;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Courtesy of @marcgravell
    /// http://code.google.com/p/protobuf-net/source/browse/trunk/protobuf-net/BufferPool.cs
    /// </summary>
    internal class BufferPool
    {
        internal static void Flush()
        {
            for (int i = 0; i < pool.Length; i++)
            {
                Interlocked.Exchange(ref pool[i], null); // and drop the old value on the floor
            }
        }

        private BufferPool() { }
        const int PoolSize = 1000; //1.45MB
        //internal const int BufferLength = 1450; //MTU size - some headers
        private static readonly object[] pool = new object[PoolSize];

        internal static byte[] GetBuffer(int bufferSize)
        {
            return bufferSize > RedisConfig.BufferPoolMaxSize 
                ? new byte[bufferSize] 
                : GetBuffer();
        }

        internal static byte[] GetBuffer()
        {
            object tmp;
            for (int i = 0; i < pool.Length; i++)
            {
                if ((tmp = Interlocked.Exchange(ref pool[i], null)) != null) 
                    return (byte[])tmp;
            }
            return new byte[RedisConfig.BufferLength];
        }

        internal static void ResizeAndFlushLeft(ref byte[] buffer, int toFitAtLeastBytes, int copyFromIndex, int copyBytes)
        {
            Debug.Assert(buffer != null);
            Debug.Assert(toFitAtLeastBytes > buffer.Length);
            Debug.Assert(copyFromIndex >= 0);
            Debug.Assert(copyBytes >= 0);

            // try doubling, else match
            int newLength = buffer.Length * 2;
            if (newLength < toFitAtLeastBytes) newLength = toFitAtLeastBytes;

            var newBuffer = new byte[newLength];
            if (copyBytes > 0)
            {
                Buffer.BlockCopy(buffer, copyFromIndex, newBuffer, 0, copyBytes);
            }
            if (buffer.Length == RedisConfig.BufferLength)
            {
                ReleaseBufferToPool(ref buffer);
            }
            buffer = newBuffer;
        }
        
        internal static void ReleaseBufferToPool(ref byte[] buffer)
        {
            if (buffer == null) return;
            if (buffer.Length == RedisConfig.BufferLength)
            {
                for (int i = 0; i < pool.Length; i++)
                {
                    if (Interlocked.CompareExchange(ref pool[i], buffer, null) == null)
                    {
                        break; // found a null; swapped it in
                    }
                }
            }
            // if no space, just drop it on the floor
            buffer = null;
        }

    }
}