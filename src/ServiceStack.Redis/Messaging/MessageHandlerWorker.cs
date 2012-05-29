using System;
using System.Threading;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using ServiceStack.Text;

namespace ServiceStack.Redis.Messaging
{
    internal class MessageHandlerWorker : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MessageHandlerWorker));

        readonly object msgLock = new object();

        private readonly IMessageHandler messageHandler;
        private readonly IRedisClientsManager clientsManager;

        public string QueueName { get; set; }

        private int status;
        public int Status
        {
            get { return status; }
        }

        private Thread bgThread;
        private int timesStarted = 0;
        public Action<MessageHandlerWorker, Exception> errorHandler { get; set; }

        private int totalMessagesProcessed;
        public int TotalMessagesProcessed
        {
            get { return totalMessagesProcessed; }
        }

        private int msgNotificationsReceived;
        public int MsgNotificationsReceived
        {
            get { return msgNotificationsReceived; }
        }

        public MessageHandlerWorker(
            IRedisClientsManager clientsManager, IMessageHandler messageHandler, string queueName,
            Action<MessageHandlerWorker, Exception> errorHandler)
        {
            this.clientsManager = clientsManager;
            this.messageHandler = messageHandler;
            this.QueueName = queueName;
            this.errorHandler = errorHandler;
        }

        public MessageHandlerWorker Clone()
        {
            return new MessageHandlerWorker(clientsManager, messageHandler, QueueName, errorHandler);
        }

        public void NotifyNewMessage()
        {
            Interlocked.Increment(ref msgNotificationsReceived);
            
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
            {
                lock (msgLock)
                {
                    Monitor.Pulse(msgLock);
                }
            }
        }

        public void Start()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started) return;
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                throw new ObjectDisposedException("MQ Host has been disposed");

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Starting, WorkerStatus.Stopped) == WorkerStatus.Stopped)
            {
                Log.Debug("Starting MQ Handler Worker: {0}...".Fmt(QueueName));

                //Should only be 1 thread past this point
                bgThread = new Thread(Run) {
                    Name = "{0}: {1}".Fmt(GetType().Name, QueueName),
                    IsBackground = true,
                };
                bgThread.Start();
            }
        }

        private void Run()
        {
            if (Interlocked.CompareExchange(ref status, WorkerStatus.Started, WorkerStatus.Starting) != WorkerStatus.Starting) return;
            timesStarted++;

            try
            {
                lock (msgLock)
                {
                    while (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started)
                    {
                        using (var mqClient = new RedisMessageQueueClient(clientsManager))
                        {
                            var msgsProcessedThisTime = messageHandler.ProcessQueue(mqClient, QueueName,
                                () => Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Started);
                            
                            totalMessagesProcessed += msgsProcessedThisTime;
                        }

                        Monitor.Wait(msgLock);
                    }
                }
            }
            catch (Exception ex)
            {
                //Ignore handling rare, but expected exceptions from KillBgThreadIfExists()
                if (ex is ThreadInterruptedException || ex is ThreadAbortException)
                {
                    Log.Warn("Received {0} in Worker: {1}".Fmt(ex.GetType().Name, QueueName));
                    return;
                }

                Stop();
                if (this.errorHandler != null) this.errorHandler(this, ex);
            }
            finally
            {
                //If it's in an invalid state, Dispose() this worker.
                if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopped, WorkerStatus.Stopping) != WorkerStatus.Stopping)
                {
                    Dispose();
                }
            }
        }

        public void Stop()
        {
            if (Interlocked.CompareExchange(ref status, 0, 0) == WorkerStatus.Disposed)
                return;

            if (Interlocked.CompareExchange(ref status, WorkerStatus.Stopping, WorkerStatus.Started) == WorkerStatus.Started)
            {
                Log.Debug("Stopping MQ Handler Worker: {0}...".Fmt(QueueName));
                Thread.Sleep(100);
                lock (msgLock)
                {
                    Monitor.Pulse(msgLock);
                }
            }
        }

        private void KillBgThreadIfExists()
        {
            if (bgThread != null && bgThread.IsAlive)
            {
                //Ideally we shouldn't get here, but lets try our hardest to clean it up
                Log.Warn("Interrupting previous Background Worker: " + bgThread.Name);
                bgThread.Interrupt();
                if (!bgThread.Join(TimeSpan.FromSeconds(3)))
                {
                    Log.Warn(bgThread.Name + " just wont die, so we're now aborting it...");
                    bgThread.Abort();
                }
                bgThread = null;
            }
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
                Thread.Sleep(100); //give it a small chance to die gracefully
                KillBgThreadIfExists();
            }
            catch (Exception ex)
            {
                Log.Error("Error Disposing MessageHandlerWorker for: " + QueueName, ex);
            }
        }

        public IMessageHandlerStats GetStats()
        {
            return messageHandler.GetStats();
        }
    }
}