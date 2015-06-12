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
using System.Linq;
using ServiceStack.Common;
using ServiceStack.Model;
using ServiceStack.Redis.Generic;
using ServiceStack.Redis.Pipeline;
using ServiceStack.Text;

namespace ServiceStack.Redis
{
    public partial class RedisClient
        : IRedisClient
    {
        public IHasNamed<IRedisSet> Sets { get; set; }

        internal class RedisClientSets
            : IHasNamed<IRedisSet>
        {
            private readonly RedisClient client;

            public RedisClientSets(RedisClient client)
            {
                this.client = client;
            }

            public IRedisSet this[string setId]
            {
                get
                {
                    return new RedisClientSet(client, setId);
                }
                set
                {
                    var col = this[setId];
                    col.Clear();
                    col.CopyTo(value.ToArray(), 0);
                }
            }
        }

        private static HashSet<string> CreateHashSet(byte[][] multiDataList)
        {
            var results = new HashSet<string>();
            foreach (var multiData in multiDataList)
            {
                results.Add(multiData.FromUtf8Bytes());
            }
            return results;
        }

        public List<string> GetSortedEntryValues(string setId, int startingFrom, int endingAt)
        {
            var sortOptions = new SortOptions { Skip = startingFrom, Take = endingAt, };
            var multiDataList = Sort(setId, sortOptions);
            return multiDataList.ToStringList();
        }

        public HashSet<string> GetAllItemsFromSet(string setId)
        {
            var multiDataList = SMembers(setId);
            return CreateHashSet(multiDataList);
        }

        public void AddItemToSet(string setId, string item)
        {
            SAdd(setId, item.ToUtf8Bytes());
        }

        public void AddRangeToSet(string setId, List<string> items)
        {
            if (setId.IsNullOrEmpty())
                throw new ArgumentNullException("setId");
            if (items == null)
                throw new ArgumentNullException("items");
            if (items.Count == 0)
                return;

            if (this.Transaction != null || this.Pipeline != null)
            {
                var queueable = this.Transaction as IRedisQueueableOperation
                    ?? this.Pipeline as IRedisQueueableOperation;

                if (queueable == null)
                    throw new NotSupportedException("Cannot AddRangeToSet() when Transaction is: " + this.Transaction.GetType().Name);

                //Complete the first QueuedCommand()
                AddItemToSet(setId, items[0]);

                //Add subsequent queued commands
                for (var i = 1; i < items.Count; i++)
                {
                    var item = items[i];
                    queueable.QueueCommand(c => c.AddItemToSet(setId, item));
                }
            }
            else 
            {
                var uSetId = setId.ToUtf8Bytes();
                var pipeline = CreatePipelineCommand();
                foreach (var item in items)
                {
                    pipeline.WriteCommand(Commands.SAdd, uSetId, item.ToUtf8Bytes());
                }
                pipeline.Flush();

                //the number of items after 
                var intResults = pipeline.ReadAllAsInts();
            }
        }

        public void RemoveItemFromSet(string setId, string item)
        {
            SRem(setId, item.ToUtf8Bytes());
        }

        public string PopItemFromSet(string setId)
        {
            return SPop(setId).FromUtf8Bytes();
        }

        public void MoveBetweenSets(string fromSetId, string toSetId, string item)
        {
            SMove(fromSetId, toSetId, item.ToUtf8Bytes());
        }

        public long GetSetCount(string setId)
        {
            return SCard(setId);
        }

        public bool SetContainsItem(string setId, string item)
        {
            return SIsMember(setId, item.ToUtf8Bytes()) == 1;
        }

        public HashSet<string> GetIntersectFromSets(params string[] setIds)
        {
            if (setIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = SInter(setIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreIntersectFromSets(string intoSetId, params string[] setIds)
        {
            if (setIds.Length == 0) return;

            SInterStore(intoSetId, setIds);
        }

        public HashSet<string> GetUnionFromSets(params string[] setIds)
        {
            if (setIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = SUnion(setIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreUnionFromSets(string intoSetId, params string[] setIds)
        {
            if (setIds.Length == 0) return;

            SUnionStore(intoSetId, setIds);
        }

        public HashSet<string> GetDifferencesFromSet(string fromSetId, params string[] withSetIds)
        {
            if (withSetIds.Length == 0)
                return new HashSet<string>();

            var multiDataList = SDiff(fromSetId, withSetIds);
            return CreateHashSet(multiDataList);
        }

        public void StoreDifferencesFromSet(string intoSetId, string fromSetId, params string[] withSetIds)
        {
            if (withSetIds.Length == 0) return;

            SDiffStore(intoSetId, fromSetId, withSetIds);
        }

        public string GetRandomItemFromSet(string setId)
        {
            return SRandMember(setId).FromUtf8Bytes();
        }
    }
}