using System;
using System.Collections.Generic;
using ServiceStack.Redis.Pipeline;

namespace ServiceStack.Redis
{
    /// <summary>
    /// Redis operation (transaction/pipeline) that allows queued commands to be completed
    /// </summary>
    public class RedisQueueCompletableOperation
    {
        internal readonly List<QueuedRedisOperation> QueuedCommands = new List<QueuedRedisOperation>();

        internal QueuedRedisOperation CurrentQueuedOperation;

        internal void BeginQueuedCommand(QueuedRedisOperation queuedRedisOperation)
        {
            if (CurrentQueuedOperation != null)
                throw new InvalidOperationException("The previous queued operation has not been commited");

            CurrentQueuedOperation = queuedRedisOperation;
        }

        internal void AssertCurrentOperation()
        {
            if (CurrentQueuedOperation == null)
                throw new InvalidOperationException("No queued operation is currently set");
        }

        protected virtual void AddCurrentQueuedOperation()
        {
            this.QueuedCommands.Add(CurrentQueuedOperation);
            CurrentQueuedOperation = null;
        }

        public virtual void CompleteVoidQueuedCommand(Action voidReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.VoidReadCommand = voidReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual  void CompleteIntQueuedCommand(Func<int> intReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.IntReadCommand = intReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteLongQueuedCommand(Func<long> longReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.LongReadCommand = longReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteBytesQueuedCommand(Func<byte[]> bytesReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.BytesReadCommand = bytesReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteMultiBytesQueuedCommand(Func<byte[][]> multiBytesReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.MultiBytesReadCommand = multiBytesReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteStringQueuedCommand(Func<string> stringReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.StringReadCommand = stringReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteMultiStringQueuedCommand(Func<List<string>> multiStringReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.MultiStringReadCommand = multiStringReadCommand;
            AddCurrentQueuedOperation();
        }

        public virtual void CompleteDoubleQueuedCommand(Func<double> doubleReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.DoubleReadCommand = doubleReadCommand;
            AddCurrentQueuedOperation();
        }


    }
}
