using System;
using System.Collections.Generic;
using System.Configuration;
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
            
            this.ErrorHandler = (mqHost, ex) =>
                Log.Error("Exception in Background Thread: {0} on mqHost: {1}".Fmt(ex.Message, ), ex);
        }

        public int NoOfThreadsPerService { get; set; }
        public int RetryCount { get; set; }
        public TimeSpan? RequestTimeOut { get; protected set; }
        public Action<IMessageService, Exception> ErrorHandler { get; set; }

        protected List<IMessageService> mqHostsBuilder;
        protected IMessageService[] mqHosts;
        
        public virtual void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn)
        {
            var redisMqHost = new RedisMqHost(redisManager, this.RetryCount, this.RequestTimeOut);
            redisMqHost.RegisterHandler(processMessageFn);
            mqHostsBuilder.Add(redisMqHost);
        }

        public virtual void RegisterHandler<T>(Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
        {
            var redisMqHost = new RedisMqHost(redisManager, this.RetryCount, this.RequestTimeOut);
            redisMqHost.RegisterHandler(processMessageFn, processExceptionEx);
            mqHostsBuilder.Add(redisMqHost);
        }

        public virtual IMessageHandlerStats GetStats()
        {
            throw new NotImplementedException();
        }

        public virtual string GetStatsDescription()
        {
            throw new NotImplementedException();
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
                    mqHost.Start();
                }
                catch (Exception ex)
                {
                    if (this.ErrorHandler != null) this.ErrorHandler(mqHost, ex);
                }
            }
        }

        public virtual IMessageFactory MessageFactory
        {
            get { throw new NotImplementedException(); }
        }

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