using System.Net.Security;

namespace ServiceStack.Redis
{
    public class RedisConfig
    {
        public static LocalCertificateSelectionCallback CertificateSelectionCallback { get; set; }

        //Example at: http://msdn.microsoft.com/en-us/library/office/dd633677(v=exchg.80).aspx 
        public static RemoteCertificateValidationCallback CertificateValidationCallback { get; set; }
    }
}