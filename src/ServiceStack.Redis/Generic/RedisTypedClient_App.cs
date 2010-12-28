using System;
using System.Collections.Generic;
using System.Linq;
using ServiceStack.Common.Utils;
using ServiceStack.Text;

namespace ServiceStack.Redis.Generic
{
	internal partial class RedisTypedClient<T>
	{
		private readonly static string RecentSortedSetKey;

		static RedisTypedClient()
		{
			RecentSortedSetKey = "recent:" + typeof(T).Name;
		}

		private static string GetChildReferenceSetKey<TChild>(object parentId)
		{
			return "ref:" + typeof(T).Name + "/" + typeof(TChild).Name + ":" + parentId;
		}

		public void StoreRelatedEntities<TChild>(object parentId, List<TChild> children) 
		{
			var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
			var childKeys = children.ConvertAll(x => x.CreateUrn());

			using (var trans = client.CreateTransaction())
			{
				//Ugly but need access to a generic constraint-free StoreAll method
				trans.QueueCommand(c => ((RedisClient)c)._StoreAll(children));
				trans.QueueCommand(c => c.AddRangeToSet(childRefKey, childKeys));

				trans.Commit();
			}
		}

		public void StoreRelatedEntities<TChild>(object parentId, params TChild[] children)
		{
			StoreRelatedEntities(parentId, new List<TChild>(children));
		}

		public List<TChild> GetRelatedEntities<TChild>(object parentId)
		{
			var childRefKey = GetChildReferenceSetKey<TChild>(parentId);
			var childKeys = client.GetAllItemsFromSet(childRefKey).ToList();

			using (var typedClient = client.GetTypedClient<TChild>())
			{
				return typedClient.GetValues(childKeys);
			}
		}

		public void AddToRecentsList(T value)
		{
			var key = value.CreateUrn();
			var nowScore = DateTime.UtcNow.ToUnixTime();
			client.AddItemToSortedSet(RecentSortedSetKey, key, nowScore);
		}

		public List<T> GetLatestFromRecentsList(int skip, int take)
		{
			var toRank = take - 1;
			var keys = client.GetRangeFromSortedSetDesc(RecentSortedSetKey, skip, toRank);
			var values = GetValues(keys);
			return values;
		}

		public List<T> GetEarliestFromRecentsList(int skip, int take)
		{
			var toRank = take - 1;
			var keys = client.GetRangeFromSortedSet(RecentSortedSetKey, skip, toRank);
			var values = GetValues(keys);
			return values;
		}
	}
}