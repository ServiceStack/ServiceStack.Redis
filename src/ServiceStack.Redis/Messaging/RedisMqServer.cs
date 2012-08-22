﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Service;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{
    /// <summary>
    /// Creates a Redis MQ Server that processes each message on its own background thread.
    /// i.e. if you register 3 handlers it will create 7 background threads:
    ///   - 1 listening to the Redis MQ Subscription, getting notified of each new message
    ///   - 3x1 Normal InQ for each message handler
    ///   - 3x1 PriorityQ for each message handler
    /// 
    /// When RedisMqServer Starts it creates a background thread subscribed to the Redis MQ Topic that
    /// listens for new incoming messages. It also starts 2 background threads for each message type:
    ///  - 1 for processing the services Priority Queue and 1 processing the services normal Inbox Queue.
    /// 
    /// Priority Queue's can be enabled on a message-per-message basis by specifying types in the 
    /// OnlyEnablePriortyQueuesForTypes property. The DisableAllPriorityQueues property disables all Queues.
    /// 
    /// The Start/Stop methods are idempotent i.e. It's safe to call them repeatedly on multiple threads 
    /// and the Redis MQ Server will only have Started or Stopped once.
    /// </summary>
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

        /// <summary>
        /// If you only want to enable priority queue handlers (and threads) for specific msg types
        /// </summary>
        public Type[] OnlyEnablePriortyQueuesForTypes { get; set; }

        /// <summary>
        /// Don't listen on any Priority Queues
        /// </summary>
        public bool DisableAllPriorityQueues
        {
            set
            {
                OnlyEnablePriortyQueuesForTypes = new Type[0];
            }
        }

        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager, null);
        }

        //Stats
        private long timesStarted = 0;
        private long noOfErrors = 0;
        private int noOfContinuousErrors = 0;
        private string lastExMsg = null;
        private int status;

        private Thread bgThread; //Subscription controller thread
        private long bgThreadCount = 0;
        public long BgThreadCount
        {
            get { return Interlocked.CompareExchange(ref bgThreadCount, 0, 0); }
        }

        private readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
            = new Dictionary<Type, IMessageHandlerFactory>();

        private MessageHandlerWorker[] workers;
        private Dictionary<string, int> queueWorkerIndexMap;


        public RedisMqServer(IRedisClientsManager clientsManager,
            int retryCount = DefaultRetryCount, TimeSpan? requestTimeOut = null)
        {
            this.clientsManager = clientsManager;
            this.RetryCount = retryCount;
            //this.RequestTimeOut = requestTimeOut;
            this.MessageFactory = new RedisMessageFactory(clientsManager);
            this.ErrorHandler = ex => Log.Error("Exception in Redis MQ Server: " + ex.Message, ex);
        }

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
            return new MessageHandlerFactory<T>(this, processMessageFn, processExceptionEx) {
                RequestFilter = this.RequestFilter,
                ResponseFilter = this.ResponseFilter,
                RetryCount = RetryCount,
            };
        }

        public void Init()
        {
            if (workers == null)
            {
                var workerBuilder = new List<MessageHandlerWorker>();

                foreach (var handlerFactory in handlerMap)
                {
                    var msgType = handlerFactory.Key;
                    var queueNames = new QueueNames(msgType);

                    if (OnlyEnablePriortyQueuesForTypes == null
                        || OnlyEnablePriortyQueuesForTypes.Any(x => x == msgType))
                    {
                        workerBuilder.Add(new MessageHandlerWorker(
                            clientsManager, 
                            handlerFactory.Value.CreateMessageHandler(), 
                            queueNames.Priority, 
                            WorkerErrorHandler));
                    }

                    workerBuilder.Add(new MessageHandlerWorker(
                        clientsManager, 
                        handlerFactory.Value.CreateMessageHandler(), 
                        queueNames.In, 
                        WorkerErrorHandler));
                }

                workers = workerBuilder.ToArray();

                queueWorkerIndexMap = new Dictionary<string, int>();
                for (var i = 0; i < workers.Length; i++)
                {
                    var worker = workers[i];
                    queueWorkerIndexMap[worker.QueueName] = i;
                }
            }
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started) return;
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            //Only 1 thread allowed past
            if (Interlocked.CompareExchange(ref status, WorkerStatus.Starting, WorkerStatus.Stopped) == WorkerStatus.Stopped) //Should only be 1 thread past this point
            {
                try
                {
                    Init();

                    if (workers == null || workers.Length == 0)
                    {
                        Log.Warn("Cannot start a MQ Server with no Message Handlers registered, ignoring.");
                        Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Starting);
                        return;
                    }

                    foreach (var worker in workers)
                    {
                        worker.Start();
                    }

                    SleepBackOffMultiplier(Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));

                    KillBgThreadIfExists();

                    bgThread = new Thread(RunLoop) {
                        IsBackground = true,
                        Name = "Redis MQ Server " + Interlocked.Increment(ref bgThreadCount)
                    };
                    bgThread.Start();
                    Log.Debug("Started Background Thread: " + bgThread.Name);

                    StartWorkerThreads();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                }
            }
        }

        private void RunLoop()
        {
            if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
            Interlocked.Increment(ref timesStarted);

            try
            {
                using (var redisClient = clientsManager.GetReadOnlyClient())
                {
                    //Record that we had a good run...
                    Interlocked.CompareExchange(ref noOfContinuousErrors, 0, noOfContinuousErrors);

                    using (var subscription = redisClient.CreateSubscription())
                    {
                        subscription.OnUnSubscribe = channel => Log.Debug("OnUnSubscribe: " + channel);

                        subscription.OnMessage = (channel, msg) => {

                            if (msg == WorkerStatus.StopCommand)
                            {
                                Log.Debug("Stop Command Issued");

                                if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Started) != WorkerStatus.Started)
                                    Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping);

                                Log.Debug("UnSubscribe From All Channels...");
                                subscription.UnSubscribeFromAllChannels(); //Un block thread.
                                return;
                            }

                            if (!string.IsNullOrEmpty(msg))
                            {
                                int workerIndex;
                                if (queueWorkerIndexMap.TryGetValue(msg, out workerIndex))
                                {
                                    var worker = workers[workerIndex];
                                    worker.NotifyNewMessage();
                                }
                            }
                        };

                        subscription.SubscribeToChannels(QueueNames.TopicIn); //blocks thread
                    }

                    StopWorkerThreads();
                }
            }
            catch (Exception ex)
            {
                lastExMsg = ex.Message;
                Interlocked.Increment(ref noOfErrors);
                Interlocked.Increment(ref noOfContinuousErrors);

                if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Started) != WorkerStatus.Started)
                    Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping);

                StopWorkerThreads();

                if (this.ErrorHandler != null) this.ErrorHandler(ex);
            }
        }

        public void Stop()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopping, WorkerStatus.Started) == WorkerStatus.Started)
            {
                Log.Debug("Stopping MQ Host...");

                //Unblock current bgthread by issuing StopCommand
                try
                {
                    using (var redis = clientsManager.GetClient())
                    {
                        redis.PublishMessage(QueueNames.TopicIn, WorkerStatus.StopCommand);
                    }
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                    Log.Warn("Could not send STOP message to bg thread: " + ex.Message);
                }
            }
        }

        void StartWorkerThreads()
        {
            Log.Debug("Starting all Redis MQ Server worker threads...");
            Array.ForEach(workers, x => x.Start());
        }

        void StopWorkerThreads()
        {
            Log.Debug("Stopping all Redis MQ Server worker threads...");
            Array.ForEach(workers, x => x.Stop());
        }

        void DisposeWorkerThreads()
        {
            Log.Debug("Disposing all Redis MQ Server worker threads...");
            if (workers != null) Array.ForEach(workers, x => x.Dispose());
        }

        void WorkerErrorHandler(MessageHandlerWorker source, Exception ex)
        {
            Log.Error("Received exception in Worker: " + source.QueueName, ex);
            for (int i = 0; i < workers.Length; i++)
            {
                var worker = workers[i];
                if (worker == source)
                {
                    Log.Debug("Starting new {0} Worker at index {1}...".Fmt(source.QueueName, i));
                    workers[i] = source.Clone();
                    workers[i].Start();
                    worker.Dispose();
                    return;
                }
            }
        }

        private void KillBgThreadIfExists()
        {
            if (bgThread != null && bgThread.IsAlive)
            {
                //give it a small chance to die gracefully
                if (!bgThread.Join(500))
                {
                    //Ideally we shouldn't get here, but lets try our hardest to clean it up
                    Log.Warn("Interrupting previous Background Thread: " + bgThread.Name);
                    bgThread.Interrupt();
                    if (!bgThread.Join(TimeSpan.FromSeconds(3)))
                    {
                        Log.Warn(bgThread.Name + " just wont die, so we're now aborting it...");
                        bgThread.Abort();
                    }
                }
                bgThread = null;
            }
        }

        readonly Random rand = new Random(Environment.TickCount);
        private void SleepBackOffMultiplier(int continuousErrorsCount)
        {
            if (continuousErrorsCount == 0) return;
            const int MaxSleepMs = 60 * 1000;

            //exponential/random retry back-off.
            var nextTry = Math.Min(
                rand.Next((int)Math.Pow(continuousErrorsCount, 3), (int)Math.Pow(continuousErrorsCount + 1, 3) + 1),
                MaxSleepMs);

            Log.Debug("Sleeping for {0}ms after {1} continuous errors".Fmt(nextTry, continuousErrorsCount));

            Thread.Sleep(nextTry);
        }

        public virtual void Dispose()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                return;

            Stop();

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopped) != WorkerStatus.Stopped)
                Interlocked.CompareExchange(ref status, WorkerStatus.Disposed, WorkerStatus.Stopping);

            try
            {
                DisposeWorkerThreads();
            }
            catch (Exception ex)
            {
                Log.Error("Error DisposeWorkerThreads(): ", ex);
            }

            try
            {
                Thread.Sleep(100); //give it a small chance to die gracefully
                KillBgThreadIfExists();
            }
            catch (Exception ex)
            {
                if (this.ErrorHandler != null) this.ErrorHandler(ex);
            }
        }

        public string GetStatus()
        {
            switch (Interlocked.CompareExchange(ref status, 0, 0))
            {
                case WorkerStatus.Disposed:
                    return "Disposed";
                case WorkerStatus.Stopped:
                    return "Stopped";
                case WorkerStatus.Stopping:
                    return "Stopping";
                case WorkerStatus.Starting:
                    return "Starting";
                case WorkerStatus.Started:
                    return "Started";
            }
            return null;
        }

        public IMessageHandlerStats GetStats()
        {
            lock (workers)
            {
                var total = new MessageHandlerStats("All Handlers");
                workers.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        public string GetStatsDescription()
        {
            lock (workers)
            {
                var sb = new StringBuilder("#MQ SERVER STATS:\n");
                sb.AppendLine("===============");
                sb.AppendLine("Current Status: " + GetStatus());
                sb.AppendLine("Listening On: " + string.Join(", ", workers.ToList().ConvertAll(x => x.QueueName).ToArray()));
                sb.AppendLine("Times Started: " + Interlocked.CompareExchange(ref timesStarted, 0, 0));
                sb.AppendLine("Num of Errors: " + Interlocked.CompareExchange(ref noOfErrors, 0, 0));
                sb.AppendLine("Num of Continuous Errors: " + Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));
                sb.AppendLine("Last ErrorMsg: " + lastExMsg);
                sb.AppendLine("===============");
                foreach (var worker in workers)
                {
                    sb.AppendLine(worker.GetStats().ToString());
                    sb.AppendLine("---------------\n");
                }
                return sb.ToString();
            }
        }
    }
}