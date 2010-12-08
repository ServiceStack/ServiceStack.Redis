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
	public interface IRedisTransaction
        : IRedisTransactionBase, IRedisQueueableOperation, IDisposable
	{


		void Commit();
		void Rollback();
	}
}