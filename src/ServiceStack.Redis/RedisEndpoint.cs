using ServiceStack.Common.Web;

namespace ServiceStack.Redis
{
    public class RedisEndPoint : EndPoint
    {
        public RedisEndPoint(string host, int port) : base(host, port)
        {
        }

        public RedisEndPoint(string host, int port, string password) : this(host,port)
        {
            this.Password = password;
        }

        public string Password { get; set; }
        public bool RequiresAuth { get { return !string.IsNullOrEmpty(Password); } }
    }
}