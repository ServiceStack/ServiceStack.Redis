using System.Text;
using NUnit.Framework;
using ServiceStack.Redis.Support;

namespace ServiceStack.Redis.Tests
{
	[TestFixture]
	public class ObjectSerializerTests
	{
		[Test]
		public void Can_serialize_object()
		{
		     var ser = new ObjectSerializer();
		     string test = "test";
		     var serialized = ser.Serialize(test);
             Assert.AreEqual(test, ser.Deserialize(serialized));
		}
	}

}