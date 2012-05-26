using System;
using System.Collections.Generic;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Service;

namespace ServiceStack.Redis.Messaging
{
    public class RedisMqServer : IMessageService
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RedisMqServer));
        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

        public int RetryCount { get; protected set; }

        public IMessageFactory MessageFactory { get; private set; }

        public Func<string, IOneWayClient> ReplyClientFactory { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic before a request is processed.
        /// Must be thread-safe.
        /// </summary>
        public Func<IMessage, IMessage> RequestFilter { get; set; }

        /// <summary>
        /// Execute global transformation or custom logic on the response.
        /// Must be thread-safe.
        /// </summary>
        public Func<object, object> ResponseFilter { get; set; }

        /// <summary>
        /// Execute global error handler logic. Must be thread-safe.
        /// </summary>
        public Action<Exception> ErrorHandler { get; set; }

        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager, null);
        }
        //Control Commands
        public const string StopCommand = "STOP";

        //States
        public const int Disposed = -1;
        public const int Stopped = 0;
        public const int Stopping = 1;
        public const int Starting = 2;
        public const int Started = 3;

        //Stats
        private long timesStarted = 0;
        private long noOfErrors = 0;
        private int noOfContinuousErrors = 0;
        private string lastExMsg = null;
        private int status;

        private readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
            = new Dictionary<Type, IMessageHandlerFactory>();

        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            RegisterHandler(processMessageFn, null);
        }

        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            if (handlerMap.ContainsKey(typeof(T)))
            {
                throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
            }

            handlerMap[typeof(T)] = CreateMessageHandlerFactory(processMessageFn, processExceptionEx);
        }

        protected IMessageHandlerFactory CreateMessageHandlerFactory<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            return new MessageHandlerFactory<T>(this, processMessageFn, processExceptionEx)
            {
                RequestFilter = this.RequestFilter,
                ResponseFilter = this.ResponseFilter,
                RetryCount = RetryCount,
            };
        }

        public IMessageHandlerStats GetStats()
        {
            throw new NotImplementedException();
        }

        public string GetStatsDescription()
        {
            throw new NotImplementedException();
        }

        public void Start()
        {
            throw new NotImplementedException();
        }

        public void Stop()
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }

    public class MessageHandlerWorker
    {
        readonly object semaphore = new object();
        private int TotalMessagesProcessed = 0;
        private int MessagesToProcess = 0;

        private IMessageHandlerFactory messageHandlerFactory;
        private IRedisClientsManager clientsManager;

        public MessageHandlerWorker(IMessageHandlerFactory messageHandlerFactory, IRedisClientsManager clientsManager)
        {
            this.messageHandlerFactory = messageHandlerFactory;
            this.clientsManager = clientsManager;
        }

        public void Pulse()
        {
            Interlocked.Increment(ref MessagesToProcess);
            Monitor.Pulse(semaphore);
        }

        private void Wait()
        {
            Monitor.Wait(semaphore);
        }

        public void Run()
        {
            
        }
    }
}