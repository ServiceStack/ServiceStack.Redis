//
// https://github.com/mythz/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2010 Liquidbit Ltd.
//
// Licensed under the same terms of Redis and ServiceStack: new BSD license.
//

using System;
using System.Collections.Generic;

namespace ServiceStack.Redis
{
    /// <summary>
    /// </summary>
    public class RedisCommandQueue : RedisQueueCompletableOperation
   {
        protected readonly RedisClient RedisClient;
  
        public RedisCommandQueue(RedisClient redisClient)
        {
            this.RedisClient = redisClient;

            if (redisClient.Transaction != null)
                throw new InvalidOperationException("An atomic command is already in use");
        }

        public void QueueCommand(Action<IRedisClient> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Action<IRedisClient> command, Action onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Action<IRedisClient> command, Action onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessVoidCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, int> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, int> command, Action<int> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, int> command, Action<int> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessIntCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, long> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, long> command, Action<long> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, long> command, Action<long> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessLongCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, bool> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, bool> command, Action<bool> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, bool> command, Action<bool> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessBoolCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, double> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, double> command, Action<double> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, double> command, Action<double> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessDoubleCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, byte[]> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, byte[]> command, Action<byte[]> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, byte[]> command, Action<byte[]> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, string> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, string> command, Action<string> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, string> command, Action<string> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, byte[][]> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, byte[][]> command, Action<byte[][]> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, byte[][]> command, Action<byte[][]> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessMultiBytesCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }


        public void QueueCommand(Func<IRedisClient, List<string>> command)
        {
            QueueCommand(command, null, null);
        }

        public void QueueCommand(Func<IRedisClient, List<string>> command, Action<List<string>> onSuccessCallback)
        {
            QueueCommand(command, onSuccessCallback, null);
        }

        public virtual void QueueCommand(Func<IRedisClient, List<string>> command, Action<List<string>> onSuccessCallback, Action<Exception> onErrorCallback)
        {
            BeginQueuedCommand(new QueuedRedisOperation
            {
                OnSuccessMultiStringCallback = onSuccessCallback,
                OnErrorCallback = onErrorCallback
            });
            command(RedisClient);
        }
    }
}