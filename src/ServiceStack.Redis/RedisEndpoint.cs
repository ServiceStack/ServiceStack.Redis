using System;
using ServiceStack.IO;

namespace ServiceStack.Redis
{
    public class RedisEndpoint : IEndpoint
    {
        public RedisEndpoint()
        {
            Host = RedisNativeClient.DefaultHost;
            Port = RedisNativeClient.DefaultPort;
            Db = RedisNativeClient.DefaultDb;

            ConnectTimeout = 0;
            SendTimeout = -1;
            ReceiveTimeout = -1;
            IdleTimeOutSecs = RedisNativeClient.DefaultIdleTimeOutSecs;
        }

        public RedisEndpoint(string host, int port, string password = null, long db = RedisNativeClient.DefaultDb)
            : this()
        {
            this.Host = host;
            this.Port = port;
            this.Password = password;
            this.Db = db;
        }

        public string Host { get; set; }
        public int Port { get; set; }
        public bool Ssl { get; set; }
        public int ConnectTimeout { get; set; }
        public int SendTimeout { get; set; }
        public int ReceiveTimeout { get; set; }
        public int IdleTimeOutSecs { get; set; }
        public long Db { get; set; }
        public string Client { get; set; }
        public string Password { get; set; }
        public bool RequiresAuth { get { return !string.IsNullOrEmpty(Password); } }
        public string NamespacePrefix { get; set; }

        protected bool Equals(RedisEndpoint other)
        {
            return string.Equals(Host, other.Host) 
                && Port == other.Port 
                && Ssl.Equals(other.Ssl) 
                && ConnectTimeout == other.ConnectTimeout 
                && SendTimeout == other.SendTimeout 
                && ReceiveTimeout == other.ReceiveTimeout 
                && IdleTimeOutSecs == other.IdleTimeOutSecs 
                && Db == other.Db 
                && string.Equals(Client, other.Client) 
                && string.Equals(Password, other.Password) 
                && string.Equals(NamespacePrefix, other.NamespacePrefix);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((RedisEndpoint)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Host != null ? Host.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Port;
                hashCode = (hashCode * 397) ^ Ssl.GetHashCode();
                hashCode = (hashCode * 397) ^ ConnectTimeout;
                hashCode = (hashCode * 397) ^ SendTimeout;
                hashCode = (hashCode * 397) ^ ReceiveTimeout;
                hashCode = (hashCode * 397) ^ IdleTimeOutSecs;
                hashCode = (hashCode * 397) ^ Db.GetHashCode();
                hashCode = (hashCode * 397) ^ (Client != null ? Client.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Password != null ? Password.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (NamespacePrefix != null ? NamespacePrefix.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}