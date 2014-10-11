using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using NUnit.Framework;
using ServiceStack.Configuration;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
    [TestFixture, Category("Integration")]
    public class SslTests
    {
        private string Host;
        private int Port;
        private string Password;
        private string connectionString;

        public SslTests()
        {
            var settings = new TextFileSettings("~/azureconfig.txt".MapProjectPath());
            Host = settings.GetString("Host");
            Port = settings.Get("Port", 6379);
            Password = settings.GetString("Password");
            connectionString = "{0}@{1}".Fmt(Password, Host);
        }

        [Test]
        public void Can_connect_to_azure_redis()
        {
            using (var client = new RedisClient(connectionString))
            {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                foo.Print();
            }
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis()
        {
            using (var client = new RedisClient(connectionString))
            {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                foo.Print();
            }
        }

        [Test]
        public void Can_connect_to_ssl_azure_redis_with_PooledClientsManager()
        {
            using (var redisManager = new PooledRedisClientManager(connectionString))
            using (var client = redisManager.GetClient())
            {
                client.Set("foo", "bar");
                var foo = client.GetValue("foo");
                foo.Print();
            }
        }

        [Test]
        public void Can_connect_to_NetworkStream()
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                SendTimeout = -1,
                ReceiveTimeout = -1,
            };

            socket.Connect(Host, 6379);

            if (!socket.Connected)
            {
                socket.Close();
                throw new Exception("Could not connect");
            }

            Stream networkStream = new NetworkStream(socket);

            SendAuth(networkStream);
        }

        [Test]
        public void Can_connect_to_Buffered_SslStream()
        {
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                SendTimeout = -1,
                ReceiveTimeout = -1,
            };

            socket.Connect(Host, Port);

            if (!socket.Connected)
            {
                socket.Close();
                throw new Exception("Could not connect");
            }

            Stream networkStream = new NetworkStream(socket);

            var sslStream = new SslStream(networkStream,
                leaveInnerStreamOpen: false,
                userCertificateValidationCallback: null,
                userCertificateSelectionCallback: null,
                encryptionPolicy: EncryptionPolicy.RequireEncryption);

            sslStream.AuthenticateAsClient(Host);

            if (!sslStream.IsEncrypted)
                throw new Exception("Could not establish an encrypted connection to " + Host);

            var bstream = new BufferedStream(sslStream, 16 * 1024);

            SendAuth(bstream);
        }

        private readonly byte[] endData = new[] { (byte)'\r', (byte)'\n' };
        private void SendAuth(Stream stream)
        {
            WriteAllToStream(stream, "AUTH".ToUtf8Bytes(), Password.ToUtf8Bytes());
            ExpectSuccess(stream);
        }

        public void WriteAllToStream(Stream stream, params byte[][] cmdWithBinaryArgs)
        {
            WriteToStream(stream, GetCmdBytes('*', cmdWithBinaryArgs.Length));

            foreach (var safeBinaryValue in cmdWithBinaryArgs)
            {
                WriteToStream(stream, GetCmdBytes('$', safeBinaryValue.Length));
                WriteToStream(stream, safeBinaryValue);
                WriteToStream(stream, endData);
            }

            stream.Flush();
        }

        public void WriteToStream(Stream stream, byte[] bytes)
        {
            stream.Write(bytes, 0, bytes.Length);
        }

        private static byte[] GetCmdBytes(char cmdPrefix, int noOfLines)
        {
            var strLines = noOfLines.ToString();
            var strLinesLength = strLines.Length;

            var cmdBytes = new byte[1 + strLinesLength + 2];
            cmdBytes[0] = (byte)cmdPrefix;

            for (var i = 0; i < strLinesLength; i++)
                cmdBytes[i + 1] = (byte)strLines[i];

            cmdBytes[1 + strLinesLength] = 0x0D; // \r
            cmdBytes[2 + strLinesLength] = 0x0A; // \n

            return cmdBytes;
        }

        protected void ExpectSuccess(Stream stream)
        {
            int c = stream.ReadByte();
            if (c == -1)
                throw new Exception("No more data");

            var s = ReadLine(stream);
            s.Print();

            if (c == '-')
                throw new Exception(s.StartsWith("ERR") && s.Length >= 4 ? s.Substring(4) : s);
        }

        protected string ReadLine(Stream stream)
        {
            var sb = new StringBuilder();

            int c;
            while ((c = stream.ReadByte()) != -1)
            {
                if (c == '\r')
                    continue;
                if (c == '\n')
                    break;
                sb.Append((char)c);
            }
            return sb.ToString();
        }
    }
}