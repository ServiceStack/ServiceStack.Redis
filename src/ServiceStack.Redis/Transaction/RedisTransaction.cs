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
	/// Adds support for Redis Transactions (i.e. MULTI/EXEC/DISCARD operations).
	/// </summary>
	public class RedisTransaction
		: RedisAllPurposePipeline, IRedisTransaction, IRedisQueueCompletableOperation
	{
		public RedisTransaction(RedisClient redisClient) : base(redisClient)
		{
			if (redisClient.CurrentTransaction != null)
				throw new InvalidOperationException("An atomic command is already in use");

			redisClient.Multi();
			redisClient.CurrentTransaction = this;
		    redisClient.CurrentPipeline = this;
		}

        /// <summary>
        /// Put "QUEUED" messages at back of queue
        /// </summary>
        /// <param name="queued"></param>
        private void QueueExpectQueued()
        {
            var op = new QueuedRedisOperation();
            op.VoidReadCommand = RedisClient.ExpectQueued;
            QueuedCommands.Insert(0, op);
        }

		public void Commit()
		{
			try
			{
				RedisClient.Exec();
                // flush send buffers
                RedisClient.FlushSendBuffer();

                //handle OK response from MULTI
                RedisClient.ExpectOk();

                // handle QUEUED responses (half of the responses should be QUEUED)
			    int numQueuedResponses = QueuedCommands.Count/2;
                for (int i = 0; i < numQueuedResponses; ++ i )
                    QueuedCommands[i].ProcessResult();
			    QueuedCommands.RemoveRange(0, QueuedCommands.Count / 2);

                //read multi-bulk result count
			    int resultCount =  RedisClient.ReadMultiDataResultCount();
                if (resultCount != QueuedCommands.Count)
                    throw new InvalidOperationException(string.Format(
                        "Invalid results received from 'EXEC', expected '{0}' received '{1}'"
                        + "\nWarning: Transaction was committed",
                        QueuedCommands.Count, resultCount));

                //receive expected results
                foreach (var queuedCommand in QueuedCommands)
                {
                    queuedCommand.ProcessResult();
                }
			}
			finally
			{
				RedisClient.CurrentTransaction = null;
			    RedisClient.CurrentPipeline = null;
				RedisClient.AddTypeIdsRegisteredDuringTransaction();
			}
		}

		public void Rollback()
		{
			if (RedisClient.CurrentTransaction == null) 
				throw new InvalidOperationException("There is no current transaction to Rollback");

			RedisClient.CurrentTransaction = null;
			RedisClient.ClearTypeIdsRegisteredDuringTransaction();
		}

		public void Dispose()
		{
            base.Dispose();
            if (RedisClient.CurrentTransaction == null) return;
		    Rollback();
		}


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
	}
}