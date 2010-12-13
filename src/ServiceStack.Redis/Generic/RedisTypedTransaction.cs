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

namespace ServiceStack.Redis.Generic
{
	/// <summary>
	/// Adds support for Redis Transactions (i.e. MULTI/EXEC/DISCARD operations).
	/// </summary>
	internal class RedisTypedTransaction<T>
		: RedisTypedPipeline<T>, IRedisTypedTransaction<T>, IRedisTransactionBase
	{
	    private int _numCommands = 0;
		internal RedisTypedTransaction(RedisTypedClient<T> redisClient)
			: base(redisClient)
		{
            redisClient.Transaction = this;
			redisClient.Multi();
		}

		/// <summary>
		/// Put "QUEUED" messages at back of queue
		/// </summary>
		public void QueueExpectQueued()
		{
			var op = new QueuedRedisOperation
			{
				VoidReadCommand = RedisClient.ExpectQueued
			};
			QueuedCommands.Insert(0, op);
		}

        public void Commit()
        {
            try
            {
                RedisClient.Exec();
                _numCommands = QueuedCommands.Count / 2;

                /////////////////////////////////////////////////////
                // Queue up reading of stock multi/exec responses

                //the first half of the responses will be "QUEUED", so insert read count operation
                // after these
                var readCountOp = new QueuedRedisOperation()
                {
                    IntReadCommand = RedisClient.ReadMultiDataResultCount,
                    OnSuccessIntCallback = handleMultiDataResultCount
                };
                QueuedCommands.Insert(_numCommands, readCountOp);

                //handle OK response from MULTI (insert at beginning)
                var readOkOp = new QueuedRedisOperation()
                {
                    VoidReadCommand = RedisClient.ExpectOk,
                };
                QueuedCommands.Insert(0, readOkOp);

                //////////////////////////////
                // flush send buffers
                RedisClient.FlushSendBuffer();

                /////////////////////////////
                //receive expected results
                foreach (var queuedCommand in QueuedCommands)
                {
                    queuedCommand.ProcessResult();
                }
            }
            finally
            {
                RedisClient.Transaction = null;
                base.ClosePipeline();
                RedisClient.AddTypeIdsRegisteredDuringTransaction();
            }
        }

        /// <summary>
        /// callback for after result count is read in
        /// </summary>
        /// <param name="count"></param>
        private void handleMultiDataResultCount(int count)
        {
            if (count != _numCommands)
                throw new InvalidOperationException(string.Format(
                    "Invalid results received from 'EXEC', expected '{0}' received '{1}'"
                    + "\nWarning: Transaction was committed",
                    _numCommands, count));
        }


		public void Rollback()
		{
			if (RedisClient.Transaction == null)
				throw new InvalidOperationException("There is no current transaction to Rollback");

			RedisClient.Transaction = null;
			RedisClient.ClearTypeIdsRegisteredDuringTransaction();
		}

		public void Dispose()
		{
			base.Dispose();
			if (RedisClient.Transaction == null) return;
			Rollback();
		}

        #region Overrides of RedisQueueCompletableOperation methods

        public override void CompleteVoidQueuedCommand(Action voidReadCommand)
        {
            base.CompleteVoidQueuedCommand(voidReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteIntQueuedCommand(Func<int> intReadCommand)
        {
            base.CompleteIntQueuedCommand(intReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteLongQueuedCommand(Func<long> longReadCommand)
        {
            base.CompleteLongQueuedCommand(longReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteBytesQueuedCommand(Func<byte[]> bytesReadCommand)
        {
            base.CompleteBytesQueuedCommand(bytesReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteMultiBytesQueuedCommand(Func<byte[][]> multiBytesReadCommand)
        {
            base.CompleteMultiBytesQueuedCommand(multiBytesReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteStringQueuedCommand(Func<string> stringReadCommand)
        {
            base.CompleteStringQueuedCommand(stringReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteMultiStringQueuedCommand(Func<List<string>> multiStringReadCommand)
        {
            base.CompleteMultiStringQueuedCommand(multiStringReadCommand);
            QueueExpectQueued();
        }
        public override void CompleteDoubleQueuedCommand(Func<double> doubleReadCommand)
        {
            base.CompleteDoubleQueuedCommand(doubleReadCommand);
            QueueExpectQueued();
        }
        #endregion
    }
}