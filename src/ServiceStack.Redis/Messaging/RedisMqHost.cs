using System;
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
	public class RedisMqHost : IMessageService
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RedisMqHost));
        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts

		public IMessageFactory MessageFactory { get; private set; }

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

        private long bgThreadCount = 0;
        public long BgThreadCount
        {
            get { return Interlocked.CompareExchange(ref bgThreadCount, 0, 0); }
        }

        public int RetryCount { get; protected set; }
		public TimeSpan? RequestTimeOut { get; protected set; }

		/// <summary>
		/// Inject your own Reply Client Factory to handle custom Message.ReplyTo urls.
		/// </summary>
        public Func<string, IOneWayClient> ReplyClientFactory { get; set; }
        
        public Action<Exception> ErrorHandler { get; set; }

        private readonly IRedisClientsManager clientsManager; //Thread safe redis client/conn factory

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.clientsManager, null);
        }

        public RedisMqHost(IRedisClientsManager clientsManager,
            int retryCount, TimeSpan? requestTimeOut)
        {
            this.clientsManager = clientsManager;
            this.RetryCount = retryCount;
            this.RequestTimeOut = requestTimeOut;
			this.MessageFactory = new RedisMessageFactory(clientsManager);
            this.ErrorHandler = ex => Log.Error("Exception in Background Thread: " + ex.Message, ex);
        }

        private readonly Dictionary<Type, IMessageHandlerFactory> handlerMap
            = new Dictionary<Type, IMessageHandlerFactory>();

        private IMessageHandler[] messageHandlers;
        private string[] inQueueNames;

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
                RetryCount = RetryCount,
				
            };
        }

        private void RunLoop()
        {
            if (Interlocked.CompareExchange(ref status, Started, Starting) != Starting) return;
            Interlocked.Increment(ref timesStarted);

            try
            {
                while (true)
                {
                    //Pass in a new MQ Client that may be used by message handlers
                    using (var mqClient = CreateMessageQueueClient())
                    {
                        foreach (var handler in messageHandlers)
                        {
                            if (Interlocked.CompareExchange(ref status, Stopped, Stopping) == Stopping)
                            {
                                Log.Debug("MQ Host is stopping, exiting RunLoop()...");
                                return;
                            }
                            if (Interlocked.CompareExchange(ref status, 0, 0) != Started)
                            {
                                Log.Error("MQ Host is in an invalid state '{0}', exiting RunLoop()...".Fmt(GetStatus()));
                                return;
                            }
                            handler.Process(mqClient);
                        }

                        //Record that we had a good run...
                        Interlocked.CompareExchange(ref noOfContinuousErrors, 0, noOfContinuousErrors);

                        var cmd = mqClient.WaitForNotifyOnAny(QueueNames.TopicIn);
                        if (cmd == StopCommand)
                        {
                            Log.Debug("Stop Command Issued");
                            if (Interlocked.CompareExchange(ref status, Stopped, Started) != Started)
                                Interlocked.CompareExchange(ref status, Stopped, Stopping);

                            return;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                lastExMsg = ex.Message;
                Interlocked.Increment(ref noOfErrors);
                Interlocked.Increment(ref noOfContinuousErrors);

                if (Interlocked.CompareExchange(ref status, Stopped, Started) != Started)
                    Interlocked.CompareExchange(ref status, Stopped, Stopping);

                if (this.ErrorHandler != null) this.ErrorHandler(ex);
            }
        }

        private Thread bgThread;

        public virtual void Start()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == Started) return;
            if (Interlocked.CompareExchange(ref status, 0, 0) == Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, Starting, Stopped) == Stopped) //Should only be 1 thread past this point
            {
                try
                {
                    Init();

                    if (this.messageHandlers == null || this.messageHandlers.Length == 0)
                    {
                        Log.Warn("Cannot start a MQ Host with no Message Handlers registered, ignoring.");
                        Interlocked.CompareExchange(ref status, Stopped, Starting);
                        return;
                    }

                    SleepBackOffMultiplier(Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));

                    KillBgThreadIfExists();

                    bgThread = new Thread(RunLoop)
                    {
                        IsBackground = true,
                        Name = "Redis MQ Host " + Interlocked.Increment(ref bgThreadCount)
                    };
                    bgThread.Start();
                    Log.Debug("Started Background Thread: " + bgThread.Name);
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                }
            }
        }

        private void KillBgThreadIfExists()
        {
            if (bgThread != null && bgThread.IsAlive)
            {
                //Ideally we shouldn't get here, but lets try our hardest to clean it up
                Log.Warn("Interrupting previous Background Thread: " + bgThread.Name);
                bgThread.Interrupt();
                if (!bgThread.Join(TimeSpan.FromSeconds(3)))
                {
                    Log.Warn(bgThread.Name + " just wont die, so we're now aborting it...");
                    bgThread.Abort();
                }
                bgThread = null;
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
            switch (Interlocked.CompareExchange(ref status, 0, 0))
            {
                case Disposed:
                    return "Disposed";
                case Stopped:
                    return "Stopped";
                case Stopping:
                    return "Stopping";
                case Starting:
                    return "Starting";
                case Started:
                    return "Started";
            }
            return null;
        }

        public IMessageHandlerStats GetStats()
        {
            lock (messageHandlers)
            {
                var total = new MessageHandlerStats("All Handlers");
                messageHandlers.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        public string GetStatsDescription()
        {
            lock (messageHandlers)
            {
                var sb = new StringBuilder("#MQ HOST STATS:\n");
                sb.AppendLine("===============");
                sb.AppendLine("Current Status: " + GetStatus());
                sb.AppendLine("Listening On: " + string.Join(", ", inQueueNames));
                sb.AppendLine("Times Started: " + Interlocked.CompareExchange(ref timesStarted, 0, 0));
                sb.AppendLine("Num of Errors: " + Interlocked.CompareExchange(ref noOfErrors, 0, 0));
                sb.AppendLine("Num of Continuous Errors: " + Interlocked.CompareExchange(ref noOfContinuousErrors, 0, 0));
                sb.AppendLine("Last ErrorMsg: " + lastExMsg);
                sb.AppendLine("===============");
                foreach (var messageHandler in messageHandlers)
                {
                    sb.AppendLine(messageHandler.GetStats().ToString());
                    sb.AppendLine("---------------\n");
                }
                return sb.ToString();
            }
        }

        public virtual void Stop()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, Stopping, Started) == Started)
            {
                Log.Debug("Stopping MQ Host...");

                //Unblock current bgthread by issuing StopCommand
                try
                {
                    using (var redis = clientsManager.GetClient())
                    {
                        redis.PublishMessage(QueueNames.TopicIn, StopCommand);
                    }
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(ex);
                    Log.Warn("Could not send STOP message to bg thread: " + ex.Message);
                }
            }
        }

    	public virtual void Dispose()
        {
            Stop();

            if (Interlocked.CompareExchange(ref status, Disposed, Stopped) != Stopped)
                Interlocked.CompareExchange(ref status, Disposed, Stopping);

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

    }

}
