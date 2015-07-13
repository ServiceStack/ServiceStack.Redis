using System;
using System.Net.Security;
using System.Threading;

namespace ServiceStack.Redis
{
    public class RedisConfig
    {
        public static Func<RedisEndpoint, RedisClient> ClientFactory = c =>
        {
            Interlocked.Increment(ref RedisState.TotalClientsCreated);
            return new RedisClient(c);
        };

        public static LocalCertificateSelectionCallback CertificateSelectionCallback { get; set; }

        //Example at: http://msdn.microsoft.com/en-us/library/office/dd633677(v=exchg.80).aspx 
        public static RemoteCertificateValidationCallback CertificateValidationCallback { get; set; }

        public static int BufferLength = 1450;

        public static int BufferPoolMaxSize = 500000;

        public static bool VerifyMasterConnections = true;

        public static int HostLookupTimeoutMs = 200;

        //Skip ServerVersion Checks by specifying Min Version number, e.g: 2.8.12 => 2812, 2.9.1 => 2910
        public static int? AssumeServerVersion;

        public static TimeSpan DeactivatedClientsExpiry = TimeSpan.FromMinutes(1);

        public static bool DisableVerboseLogging = false;
    }
}