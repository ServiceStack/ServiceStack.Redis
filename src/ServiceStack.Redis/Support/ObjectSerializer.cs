using System.Collections;
using System.Collections.Generic;
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
		private readonly BinaryFormatter bf = new BinaryFormatter();
 

		/// <summary>
		/// 
		/// </summary>
		/// <param name="values">array of serializable objects</param>
		/// <returns></returns>
		public List<byte[]> Serialize(object[] values)
		{
			var rc = new List<byte[]>();
			foreach (var value in values)
			{
				var bytes = Serialize(value);
				if (bytes != null)
					rc.Add(bytes);
			}
			return rc;
		}

		/// <summary>
		///  Serialize object to buffer
		/// </summary>
		/// <param name="value">serializable object</param>
		/// <returns></returns>
		public byte[] Serialize(object value)
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
		public object Deserialize(byte[] someBytes)
		{
			if (someBytes == null)
				return null;
			var memoryStream = new MemoryStream();
			memoryStream.Write(someBytes, 0, someBytes.Length);
			memoryStream.Seek(0, 0);
			var de = bf.Deserialize(memoryStream);
			return de;
		}
		/// <summary>
		/// deserialize an array of byte arrays
		/// </summary>
		/// <param name="byteArray"></param>
		/// <returns></returns>
		public IList Deserialize(byte[][] byteArray)
		{
			IList rc = new ArrayList();
			foreach (var someBytes in byteArray)
			{
				var obj = Deserialize(someBytes);
				if (obj != null)
					rc.Add(obj);
			}
			return rc;
		}
	}
}