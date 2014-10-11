using System.Net.Security;

namespace ServiceStack.Redis
{
    public class RedisConfig
    {
        public static LocalCertificateSelectionCallback CertificateSelectionCallback { get; set; }
        public static RemoteCertificateValidationCallback CertificateValidationCallback { get; set; }
    }
}