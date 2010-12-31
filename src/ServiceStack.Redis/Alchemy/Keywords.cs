using ServiceStack.Text;

namespace ServiceStack.Redis.Alchemy
{
	public static class Keywords
	{
            
        public readonly static byte[] Table = "TABLE".ToUtf8Bytes();
		public readonly static byte[] Index = "INDEX".ToUtf8Bytes();

		public readonly static byte[] On = "ON".ToUtf8Bytes();

		public readonly static byte[] To = "TO".ToUtf8Bytes();

		public readonly static byte[] Mysql = "MYSQL".ToUtf8Bytes();

		public readonly static byte[] File = "FILE".ToUtf8Bytes();
		public readonly static byte[] Into = "INTO".ToUtf8Bytes();

		public readonly static byte[] Values = "VALUES".ToUtf8Bytes();
		public readonly static byte[] From = "FROM".ToUtf8Bytes();
		public readonly static byte[] Where = "WHERE".ToUtf8Bytes();

	
	}
}