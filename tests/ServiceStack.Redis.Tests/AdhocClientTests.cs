using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using NUnit.Framework;
using ServiceStack.Common.Utils;
using ServiceStack.ServiceClient.Web;
using ServiceStack.Redis;
using System.Text;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
	[TestFixture]
	public class AdhocClientTests
	{
		[Test]
		public void Search_Test()
		{
			using (var client = new RedisClient(TestConfig.SingleHost))
			{
				const string cacheKey = "urn+metadata:All:SearchProProfiles?SwanShinichi Osawa /0/8,0,0,0";
				const long value = 1L;
				client.Set(cacheKey, value);
				var result = client.Get<long>(cacheKey);

				Assert.That(result, Is.EqualTo(value));
			}
		}

        public string CalculateMD5Hash(string input)
        {
            // step 1, calculate MD5 hash from input
            var md5 = MD5.Create();
            byte[] inputBytes = Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);
 
            // step 2, convert byte array to hex string
            var sb = new StringBuilder();
            for (int i = 0; i < hash.Length; i++)
            {
                sb.Append(hash[i].ToString("X2"));
            }
            return sb.ToString();
        }

        [Test]
        public void PrintHash()
        {
            Console.WriteLine(CalculateMD5Hash("X5O!P%@AP[4\\PZX54(P^)7CC)7}$EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*"));
        }

        [Test]
        public void Write_Known_Virus_JsonResult()
        {
            //var html = "http://www.virustotal.com/latest-report.html?resource=44D88612FEA8A8F36DE82E1278ABB02F".DownloadJsonFromUrl();
            var json = "https://www.virustotal.com/api/get_file_report.json?resource=44D88612FEA8A8F36DE82E1278ABB02F".DownloadJsonFromUrl();
            Console.WriteLine(json);
        }

        [Test]
        public void Write_Unknown_file_JsonResult()
        {
            var json = "http://www.virustotal.com/latest-report.html?resource=AAAA8612FEA8A8F36DE82E1278ABB02F".DownloadJsonFromUrl();
            Console.WriteLine(json);
        }

		[Test]
		public void Can_infer_utf8_bytes()
		{
			var cmd = "GET" + 2 + "\r\n";
			var cmdBytes = System.Text.Encoding.UTF8.GetBytes(cmd);

			var hex = BitConverter.ToString(cmdBytes);

			Console.WriteLine(hex);

			Console.WriteLine(BitConverter.ToString("G".ToUtf8Bytes()));
			Console.WriteLine(BitConverter.ToString("E".ToUtf8Bytes()));
			Console.WriteLine(BitConverter.ToString("T".ToUtf8Bytes()));
			Console.WriteLine(BitConverter.ToString("2".ToUtf8Bytes()));
			Console.WriteLine(BitConverter.ToString("\r".ToUtf8Bytes()));
			Console.WriteLine(BitConverter.ToString("\n".ToUtf8Bytes()));

			var bytes = new[] { (byte)'\r', (byte)'\n', (byte)'0', (byte)'9', };
			Console.WriteLine(BitConverter.ToString(bytes));
		}

		[Test]
		public void Convert_int()
		{
			var test = 1234;
			Console.WriteLine(BitConverter.ToString(1234.ToString().ToUtf8Bytes()));
		}

		private static byte[] GetCmdBytes1(char cmdPrefix, int noOfLines)
		{
			var cmd = cmdPrefix.ToString() + noOfLines.ToString() + "\r\n";
			return cmd.ToUtf8Bytes();
		}

		private static byte[] GetCmdBytes2(char cmdPrefix, int noOfLines)
		{
			var strLines = noOfLines.ToString();
			var cmdBytes = new byte[1 + strLines.Length + 2];
			cmdBytes[0] = (byte)cmdPrefix;

			for (var i = 0; i < strLines.Length; i++)
				cmdBytes[i + 1] = (byte)strLines[i];

			cmdBytes[cmdBytes.Length - 2] = 0x0D; // \r
			cmdBytes[cmdBytes.Length - 1] = 0x0A; // \n

			return cmdBytes;
		}

		[Test]
		public void Compare_GetCmdBytes()
		{
			var res1 = GetCmdBytes1('$', 1234);
			var res2 = GetCmdBytes2('$', 1234);

			Console.WriteLine(BitConverter.ToString(res1));
			Console.WriteLine(BitConverter.ToString(res2));

			var ticks1 = PerfUtils.Measure(1000000, () => GetCmdBytes1('$', 2));
			var ticks2 = PerfUtils.Measure(1000000, () => GetCmdBytes2('$', 2));

			Console.WriteLine("{0} : {1} = {2}", ticks1, ticks2, ticks1 / (double)ticks2);
		}

	}
}