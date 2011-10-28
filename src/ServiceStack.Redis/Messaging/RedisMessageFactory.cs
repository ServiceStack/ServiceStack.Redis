using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
	public class RedisMessageFactory : IMessageFactory
	{
		private readonly IRedisClientsManager clientsManager;

		public RedisMessageFactory(IRedisClientsManager clientsManager)
		{
			this.clientsManager = clientsManager;
		}

		public IMessageQueueClient CreateMessageQueueClient()
		{
			return new RedisMessageQueueClient(clientsManager);
		}

		public IMessageProducer CreateMessageProducer()
		{
			return new RedisMessageProducer(clientsManager);
		}

		public void Dispose()
		{
		}
	}
}