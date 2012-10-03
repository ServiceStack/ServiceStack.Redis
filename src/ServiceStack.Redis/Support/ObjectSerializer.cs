using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace ServiceStack.Redis.Support
{
	/// <summary>
	/// serialize/deserialize arbitrary objects
	/// (objects must be serializable)
	/// </summary>
	public class ObjectSerializer : ISerializer
	{
		protected readonly BinaryFormatter bf = new BinaryFormatter();
 


		/// <summary>
		///  Serialize object to buffer
		/// </summary>
		/// <param name="value">serializable object</param>
		/// <returns></returns>
		public virtual byte[] Serialize(object value)
		{
			if (value == null)
				return null;
			var memoryStream = new MemoryStream();
			memoryStream.Seek(0, 0);
			bf.Serialize(memoryStream, value);
			return memoryStream.ToArray();
		}

		/// <summary>
		///     Deserialize buffer to object
		/// </summary>
		/// <param name="someBytes">byte array to deserialize</param>
		/// <returns></returns>
		public virtual object Deserialize(byte[] someBytes)
		{
			if (someBytes == null)
				return null;
			var memoryStream = new MemoryStream();
			memoryStream.Write(someBytes, 0, someBytes.Length);
			memoryStream.Seek(0, 0);
			var de = bf.Deserialize(memoryStream);
			return de;
		}
	}
}