#if NETSTANDARD1_3
using System;
using System.IO;
using System.Net.Sockets;

namespace ServiceStack.Redis
{
    public sealed class BufferedStream : Stream
    {
        NetworkStream networkStream;

        public BufferedStream(Stream stream)
            : this(stream, 0)
        {
        }

        public BufferedStream(Stream stream, int bufferSize)
        {
            networkStream = stream as NetworkStream;

            if (networkStream == null)
                throw new ArgumentNullException("stream");
        }


/*        public BufferedStream(Stream stream)
            : this(stream, 0)
        {
        }

        public BufferedStream(Stream stream, int bufferSize) : base (stream)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");
        }
        */
        public override bool CanRead
        {
            get { return networkStream.CanRead; }
        }

        public override bool CanSeek
        {
            get { return networkStream.CanSeek; }
        }

        public override bool CanWrite
        {
            get { return networkStream.CanWrite; }
        }

        public override long Position
        {
            get { return networkStream.Position; }
            set { networkStream.Position = value; }
        }

        public override long Length
        {
            get { return networkStream.Length; }
        }

        public override int Read(byte[] buffer, int offset, int length)
        {
            return networkStream.Read(buffer, offset, length);
        }

        public override void Write(byte[] buffer, int offset, int length)
        {
            networkStream.Write(buffer, offset, length);
        }

        public override void Flush()
        {
            networkStream.Flush();
        }

        public override void SetLength(long length)
        {
            networkStream.SetLength(length);
        }

        public override long Seek(long position, SeekOrigin origin)
        {
            return networkStream.Seek(position, origin);
        }
    }
}
#endif