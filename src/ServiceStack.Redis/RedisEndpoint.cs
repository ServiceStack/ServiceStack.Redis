using ServiceStack.IO;

namespace ServiceStack.Redis
{
    public class RedisEndpoint : IEndpoint
    {
        public RedisEndpoint(string host, int port, string password=null, long db=RedisNativeClient.DefaultDb)
        {
            this.Host = host;
            this.Port = port;
            this.Password = password;
            this.Db = db;

            SendTimeout = -1;
            ReceiveTimeout = -1;
            IdleTimeOutSecs = RedisNativeClient.DefaultIdleTimeOutSecs;
        }

        public string Host { get; set; }
        public int Port { get; set; }
        public int SendTimeout { get; set; }
        public int ReceiveTimeout { get; set; }
        public int IdleTimeOutSecs { get; set; }
        public long Db { get; set; }
        public string Password { get; set; }
        public bool Ssl { get; set; }
        public bool RequiresAuth { get { return !string.IsNullOrEmpty(Password); } }
    }
}