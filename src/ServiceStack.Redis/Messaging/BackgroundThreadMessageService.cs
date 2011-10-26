using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;

namespace ServiceStack.Redis.Messaging
{
	public class BackgroundThreadMessageService : IMessageService
	{
		public const int Stopped = 0;
		public const int Starting = 1;
		public const int Started = 2;

		private static readonly ILog Log = LogManager.GetLogger(typeof(BackgroundThreadMessageService));

		private static long bgThreadCount = 0;
		private static long timesStarted = 0;
		private static long timesAborted = 0;
		private int status;
		private Exception lastEx = null;

		public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

		public int RetryCount { get; protected set; }
		public TimeSpan? RequestTimeOut { get; protected set; }

		public int PoolSize { get; protected set; } //use later

		private readonly IRedisClientsManager clientsManager;

		public IMessageQueueClient CreateMessageQueueClient()
		{
			return new RedisMessageQueueClient(this.clientsManager, null);
		}

		public BackgroundThreadMessageService(IRedisClientsManager clientsManager, 
			int retryAttempts, TimeSpan? requestTimeOut)
		{
			this.clientsManager = clientsManager;
			this.RetryCount = retryAttempts;
			this.RequestTimeOut = requestTimeOut;
		}

		private readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
			= new Dictionary<Type, IMessageHandlerFactory>();

		private IMessageHandler[] messageHandlers;
		private string[] inQueueNames;

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
		{
			RegisterHandler(processMessageFn, null);
		}

		public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<Exception> processExceptionEx)
		{
			if (handlerMap.ContainsKey(typeof(T)))
			{
				throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
			}

			handlerMap[typeof(T)] = CreateMessageHandlerFactory(processMessageFn, processExceptionEx);
		}

		protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<Exception> processExceptionEx)
		{
			return new MessageHandlerFactory<T>(this, processMessageFn, processExceptionEx);
		}

		private void RunLoop()
		{
			if (Interlocked.CompareExchange(ref status, Started, Starting) != Starting) return;
			Interlocked.Increment(ref timesStarted);

			try
			{
				while (true)
				{
					//Pass in new MQ Client that may be used by message handlers
					using (var mqClient = CreateMessageQueueClient())
					{
						foreach (var handler in messageHandlers)
						{
							if (status != Started)
							{
								Log.Debug("MQ Host was stopped, exiting RunLoop()...");
								return;
							}
							handler.Process(mqClient);
						}

						mqClient.WaitForNotifyOnAny(QueueNames.Topic);
					}
				}
			}
			catch (Exception ex)
			{
				lastEx = ex;
				timesAborted++;
				Log.Debug("Exception in Background Thread: " + ex.Message);
				status = Stopped;
			}
		}

		private Thread bgThread;

		public virtual void Start()
		{
			if (status == Started) return;
			if (Interlocked.CompareExchange(ref status, Starting, Stopped) == Stopped)
			{
				Init();

				if (this.messageHandlers == null || this.messageHandlers.Length == 0)
				{
					Log.Warn("Cannot start a MQ Host with no Message Handlers registered, ignoring.");
					status = Stopped;
					return;
				}

				if (bgThread != null)
				{
					bgThread.Interrupt();
					if (!bgThread.Join(TimeSpan.FromSeconds(3)))
					{
						bgThread.Abort();
					}
					bgThread = null;
				}

				bgThread = new Thread(RunLoop) 
				{
					IsBackground = true,
					Name = "Redis MQ Host " + Interlocked.Increment(ref bgThreadCount)
				};
				bgThread.Start();
				Log.Debug("Started Background Thread: " + bgThread.Name);
			}
		}

		private void Init()
		{
			if (this.messageHandlers == null)
			{
				this.messageHandlers = this.handlerMap.Values.ToList()
					.ConvertAll(x => x.CreateMessageHandler()).ToArray();
			}
			if (inQueueNames == null)
			{
				inQueueNames = this.handlerMap.Keys.ToList()
					.ConvertAll(x => new QueueNames(x).In).ToArray();
			}
		}

		public string GetStatus()
		{
			switch (status)
			{
				case Stopped:
					return "Stopped";
				case Starting:
					return "Starting";
				case Started:
					return "Started";
			}
			return null;
		}

		public string GetStats()
		{
			var sb = new StringBuilder("#MQ HOST STATS:\n");
			sb.AppendLine("===============");
			sb.AppendLine("Current Status: " + GetStatus());
			sb.AppendLine("Listening On: " + string.Join(", ", inQueueNames));
			sb.AppendLine("Times Started: " + timesStarted);
			sb.AppendLine("Times Aborted: " + timesAborted);
			sb.AppendLine("Last ErrorMsg: " + (lastEx == null ? "None" : lastEx.Message));
			sb.AppendLine("===============");
			foreach (var messageHandler in messageHandlers)
			{
				sb.AppendLine(messageHandler.GetStats());
				sb.AppendLine("---------------");
			}
			return sb.ToString();
		}

		public virtual void Stop()
		{
			Log.Debug("Stopping MQ Host...");
			Interlocked.CompareExchange(ref status, Stopped, Started);
		}

		public virtual void Dispose()
		{
			Stop();
		}

	}

}
