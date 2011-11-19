using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using ServiceStack.Common;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{
    public class RedisMqHostPool : IMessageService
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(RedisMqHostPool));

        public const int DefaultNoOfThreadsPerService = 1;
        public const int DefaultRetryCount = 2; //3 tries in total

        private readonly IRedisClientsManager redisManager;

        public RedisMqHostPool(IRedisClientsManager redisManager,
            int? retryCount = 0, TimeSpan? requestTimeOut = null)
        {
            this.NoOfThreadsPerService = DefaultNoOfThreadsPerService;
            this.RetryCount = retryCount.GetValueOrDefault(DefaultRetryCount);
            this.RequestTimeOut = requestTimeOut;
            this.mqHostsBuilder = new List<IMessageService>();
            this.redisManager = redisManager;
            this.MessageFactory = new RedisMessageFactory(redisManager);

            this.ErrorHandler = (mqHost, ex) =>
                Log.Error("Exception in Background Thread: {0} on mqHost: {1}".Fmt(ex.Message, ((RedisMqHost)mqHost).Title), ex);
        }

        public int NoOfThreadsPerService { get; set; }
        public int RetryCount { get; set; }
        public TimeSpan? RequestTimeOut { get; protected set; }
        public Action<IMessageService, Exception> ErrorHandler { get; set; }

        public long BgThreadCount
        {
            get { return mqHosts.Sum(x => ((RedisMqHost) x).BgThreadCount); }
        }

        protected List<IMessageService> mqHostsBuilder;
        protected IMessageService[] mqHosts;

        public virtual void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, int? noOfThreads = null)
        {
            (noOfThreads ?? this.NoOfThreadsPerService).Times(x =>
            {
                var redisMqHost = new RedisMqHost(redisManager, this.RetryCount, this.RequestTimeOut);
                redisMqHost.RegisterHandler(processMessageFn);
                mqHostsBuilder.Add(redisMqHost);
            });
        }

        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            RegisterHandler(processMessageFn, (int?)null);
        }

        public virtual void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx, int? noOfThreads = null)
        {
            (noOfThreads ?? this.NoOfThreadsPerService).Times(x =>
            {
                var redisMqHost = new RedisMqHost(redisManager, this.RetryCount, this.RequestTimeOut);
                redisMqHost.RegisterHandler(processMessageFn, processExceptionEx);
                mqHostsBuilder.Add(redisMqHost);
            });
        }

        public void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            RegisterHandler(processMessageFn, processExceptionEx, null);
        }

        public IMessageQueueClient CreateMessageQueueClient()
        {
            return new RedisMessageQueueClient(this.redisManager, null);
        }

        public virtual string GetStatus()
        {
            if (mqHosts == null) return null;
            var statusSet = new HashSet<string>();
            lock (mqHosts)
            {
                foreach (var mqHost in mqHosts)
                {
                    statusSet.Add(((RedisMqHost)mqHost).GetStatus());
                }
            }
            var allStatuses = string.Join(",", statusSet.ToArray());
            return allStatuses;
        }

        public virtual IMessageHandlerStats GetStats()
        {
            if (mqHosts == null) return null;
            lock (mqHosts)
            {
                var total = new MessageHandlerStats("All Handlers");
                mqHosts.ToList().ForEach(x => total.Add(x.GetStats()));
                return total;
            }
        }

        public virtual string GetStatsDescription()
        {
            if (mqHosts == null) return null;
            lock (mqHosts)
            {
                var sb = new StringBuilder();
                mqHosts.ToList().ForEach(x => sb.AppendFormat(x.GetStatsDescription() + "\n\n"));
                return sb.ToString();
            }
        }

        public virtual void Start()
        {
            //First call should be started on a single thread, i.e. in Global.asax Application_Start()
            if (mqHosts == null)
            {
                if (mqHostsBuilder.Count == 0)
                    throw new ConfigurationException("No Handler's were registered.");

                mqHosts = mqHostsBuilder.ToArray();
            }

            foreach (var mqHost in mqHosts)
            {
                try
                {
                    mqHost.Start();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(mqHost, ex);
                }
            }
        }

        public virtual void Stop()
        {
            if (mqHosts == null) return;

            foreach (var mqHost in mqHosts)
            {
                try
                {
                    mqHost.Stop();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(mqHost, ex);
                }
            }
        }

        public IMessageFactory MessageFactory { get; set; }

        public virtual void Dispose()
        {
            if (mqHosts == null) return;

            foreach (var mqHost in mqHosts)
            {
                try
                {
                    mqHost.Dispose();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(mqHost, ex);
                }
            }
        }
    }
}