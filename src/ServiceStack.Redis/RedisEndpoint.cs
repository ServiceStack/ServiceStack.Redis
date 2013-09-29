using ServiceStack.IO;

namespace ServiceStack.Redis
{
    public class RedisEndpoint : IEndpoint
    {
        public RedisEndpoint(string host, int port)
        {
            this.Host = host;
            this.Port = port;
        }

        public RedisEndpoint(string host, int port, string password) : this(host,port)
        {
            this.Password = password;
        }

        public string Host { get; private set; }
        public int Port { get; private set; }
        public string Password { get; set; }
        public bool RequiresAuth { get { return !string.IsNullOrEmpty(Password); } }
    }
}