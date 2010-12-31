using ServiceStack.Text;

namespace ServiceStack.Redis.Alchemy
{
	public static class Commands
	{
        public readonly static byte[] Create = "CREATE".ToUtf8Bytes();
		public readonly static byte[] Drop = "DROP".ToUtf8Bytes();
		public readonly static byte[] Dump = "DUMP".ToUtf8Bytes();
		public readonly static byte[] Insert = "INSERT".ToUtf8Bytes();
		public readonly static byte[] ScanSelect = "SCANSELECT".ToUtf8Bytes();
		public readonly static byte[] Update = "UPDATE".ToUtf8Bytes();
		public readonly static byte[] Delete = "DELETE".ToUtf8Bytes();
		public readonly static byte[] Lua = "LUA".ToUtf8Bytes();
		public readonly static byte[] Return = "RETURN".ToUtf8Bytes();
		public readonly static byte[] Size = "SIZE".ToUtf8Bytes();
	}
}