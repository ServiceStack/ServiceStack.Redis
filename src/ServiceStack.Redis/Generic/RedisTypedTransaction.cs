//
// https://github.com/ServiceStack/ServiceStack.Redis
// ServiceStack.Redis: ECMA CLI Binding to the Redis key-value storage system
//
// Authors:
//   Demis Bellot (demis.bellot@gmail.com)
//
// Copyright 2013 Service Stack LLC. All Rights Reserved.
//
// Licensed under the same terms of ServiceStack.
//

using System;
using System.Collections.Generic;
using ServiceStack.Redis.Pipeline;

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

        }

        protected override void Init()
        {
            //start pipelining
            base.Init();
            //queue multi command
            RedisClient.Multi();
            //set transaction
            RedisClient.Transaction = this;
        }

        /// <summary>
        /// Put "QUEUED" messages at back of queue
        /// </summary>
        /// <param name="queued"></param>
        private void QueueExpectQueued()
        {
            QueuedCommands.Insert(0, new QueuedRedisOperation()
            {
                VoidReadCommand = RedisClient.ExpectQueued
            });
        }


        /// <summary>
        /// Issue exec command (not queued)
        /// </summary>
        private void Exec()
        {
            RedisClient.Exec();
            RedisClient.FlushSendBuffer();

        }

        public bool Commit()
        {
            bool rc = true;
            try
            {
                _numCommands = QueuedCommands.Count / 2;

                //insert multi command at beginning
                QueuedCommands.Insert(0, new QueuedRedisCommand()
                {
                    VoidReturnCommand = r => Init(),
                    VoidReadCommand = RedisClient.ExpectOk,
                });


                //the first half of the responses will be "QUEUED",
                // so insert reading of multiline after these responses
                QueuedCommands.Insert(_numCommands + 1, new QueuedRedisOperation()
                {
                    IntReadCommand = RedisClient.ReadMultiDataResultCount,
                    OnSuccessIntCallback = handleMultiDataResultCount
                });

                // add Exec command at end (not queued)
                QueuedCommands.Add(new RedisCommand()
                {
                    VoidReturnCommand = r => Exec()
                });

                //execute transaction
                Exec();

                /////////////////////////////
                //receive expected results
                foreach (var queuedCommand in QueuedCommands)
                {
                    queuedCommand.ProcessResult();
                }
            }
            catch (RedisTransactionFailedException)
            {
                rc = false;
            }
            finally
            {
                RedisClient.Transaction = null;
                ClosePipeline();
                RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }
            return rc;
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
            RedisClient.ClearTypeIdsRegisteredDuringPipeline();
        }

        public bool Replay()
        {
            bool rc = true;
            try
            {
                Execute();

                /////////////////////////////
                //receive expected results
                foreach (var queuedCommand in QueuedCommands)
                {
                    queuedCommand.ProcessResult();
                }
            }
            catch (RedisTransactionFailedException)
            {
                rc = false;
            }
            finally
            {

                RedisClient.Transaction = null;
                ClosePipeline();
                RedisClient.AddTypeIdsRegisteredDuringPipeline();
            }
            return rc;
        }

        public void Dispose()
        {
            base.Dispose();
            if (RedisClient.Transaction == null) return;
            Rollback();
        }

        #region Overrides of RedisQueueCompletableOperation methods

        protected override void AddCurrentQueuedOperation()
        {
            base.AddCurrentQueuedOperation();
            QueueExpectQueued();
        }
        #endregion
    }
}