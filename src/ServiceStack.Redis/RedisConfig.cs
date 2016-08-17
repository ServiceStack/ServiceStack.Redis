using System;
using System.Net.Security;
using System.Threading;

namespace ServiceStack.Redis
{
    public class RedisConfig
    {
        //redis-server defaults:
        public const long DefaultDb = 0;
        public const int DefaultPort = 6379;
        public const int DefaultPortSsl = 6380;
        public const int DefaultPortSentinel = 26379;
        public const string DefaultHost = "localhost";

        /// <summary>
        /// Factory used to Create `RedisClient` instances
        /// </summary>
        public static Func<RedisEndpoint, RedisClient> ClientFactory = c =>
        {
            Interlocked.Increment(ref RedisState.TotalClientsCreated);
            return new RedisClient(c);
        };

        /// <summary>
        /// The default RedisClient Socket ConnectTimeout (default -1, None)
        /// </summary>
        public static int DefaultConnectTimeout = -1;

        /// <summary>
        /// The default RedisClient Socket SendTimeout (default -1, None)
        /// </summary>
        public static int DefaultSendTimeout = -1;

        /// <summary>
        /// The default RedisClient Socket ReceiveTimeout (default -1, None)
        /// </summary>
        public static int DefaultReceiveTimeout = -1;

        /// <summary>
        /// Default Idle TimeOut before a connection is considered to be stale (default 240 secs)
        /// </summary>
        public static int DefaultIdleTimeOutSecs = 240;

        /// <summary>
        /// The default RetryTimeout for auto retry of failed operations (default 10,000ms)
        /// </summary>
        public static int DefaultRetryTimeout = 10 * 1000;

        /// <summary>
        /// Default Max Pool Size for Pooled Redis Client Managers (default none)
        /// </summary>
        public static int? DefaultMaxPoolSize;

        /// <summary>
        /// The BackOff multiplier failed Auto Retries starts from (default 10ms)
        /// </summary>
        public static int BackOffMultiplier = 10;

        /// <summary>
        /// The Byte Buffer Size to combine Redis Operations within (default 1450 bytes)
        /// </summary>
        public static int BufferLength = 1450;

        /// <summary>
        /// The Byte Buffer Size for Operations to use a byte buffer pool (default 500kb)
        /// </summary>
        public static int BufferPoolMaxSize = 500000;

        /// <summary>
        /// Whether Connections to Master hosts should be verified they're still master instances (default true)
        /// </summary>
        public static bool VerifyMasterConnections = true;

        /// <summary>
        /// The ConnectTimeout on clients used to find the next available host (default 200ms)
        /// </summary>
        public static int HostLookupTimeoutMs = 200;

        /// <summary>
        /// Skip ServerVersion Checks by specifying Min Version number, e.g: 2.8.12 => 2812, 2.9.1 => 2910
        /// </summary>
        public static int? AssumeServerVersion;

        /// <summary>
        /// How long to hold deactivated clients for before disposing their connection (default 1 min)
        /// Dispose of deactivated Clients immediately with TimeSpan.Zero
        /// </summary>
        public static TimeSpan DeactivatedClientsExpiry = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Whether Debug Logging should log detailed Redis operations (default false)
        /// </summary>
        public static bool DisableVerboseLogging = false;

        //Example at: http://msdn.microsoft.com/en-us/library/office/dd633677(v=exchg.80).aspx 
        public static LocalCertificateSelectionCallback CertificateSelectionCallback { get; set; }
        public static RemoteCertificateValidationCallback CertificateValidationCallback { get; set; }

        /// <summary>
        /// Resets Redis Config and Redis Stats back to default values
        /// </summary>
        public static void Reset()
        {
            RedisStats.Reset();

            DefaultConnectTimeout = -1;
            DefaultSendTimeout = -1;
            DefaultReceiveTimeout = -1;
            DefaultRetryTimeout = 10 * 1000;
            DefaultIdleTimeOutSecs = 240;
            DefaultMaxPoolSize = null;
            BackOffMultiplier = 10;
            BufferLength = 1450;
            BufferPoolMaxSize = 500000;
            VerifyMasterConnections = true;
            HostLookupTimeoutMs = 200;
            AssumeServerVersion = null;
            DeactivatedClientsExpiry = TimeSpan.FromMinutes(1);
            DisableVerboseLogging = false;
            CertificateSelectionCallback = null;
            CertificateValidationCallback = null;
        }
    }
}