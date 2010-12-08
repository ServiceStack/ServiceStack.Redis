using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

        private void AddCurrentQueuedOperation()
        {
            this.QueuedCommands.Add(CurrentQueuedOperation);
            CurrentQueuedOperation = null;
        }

        public void CompleteVoidQueuedCommand(Action voidReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.VoidReadCommand = voidReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteIntQueuedCommand(Func<int> intReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.IntReadCommand = intReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteLongQueuedCommand(Func<long> longReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.LongReadCommand = longReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteBytesQueuedCommand(Func<byte[]> bytesReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.BytesReadCommand = bytesReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteMultiBytesQueuedCommand(Func<byte[][]> multiBytesReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.MultiBytesReadCommand = multiBytesReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteStringQueuedCommand(Func<string> stringReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.StringReadCommand = stringReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteMultiStringQueuedCommand(Func<List<string>> multiStringReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.MultiStringReadCommand = multiStringReadCommand;
            AddCurrentQueuedOperation();
        }

        public void CompleteDoubleQueuedCommand(Func<double> doubleReadCommand)
        {
            AssertCurrentOperation();

            CurrentQueuedOperation.DoubleReadCommand = doubleReadCommand;
            AddCurrentQueuedOperation();
        }


    }
}
