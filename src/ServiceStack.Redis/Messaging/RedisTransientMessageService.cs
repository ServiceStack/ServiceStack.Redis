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
using ServiceStack.Common.Extensions;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
	public class RedisTransientMessageService
		: TransientMessageServiceBase
	{
		private readonly RedisTransientMessageFactory messageFactory;

		public RedisTransientMessageService(int retryAttempts, TimeSpan? requestTimeOut,
			RedisTransientMessageFactory messageFactory)
			: base(retryAttempts, requestTimeOut)
		{
			messageFactory.ThrowIfNull("messageFactory");
			this.messageFactory = messageFactory;
		}

		public override IMessageFactory MessageFactory
		{
			get { return messageFactory; }
		}
	}

}