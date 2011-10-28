using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using NUnit.Framework;
using ServiceStack.Text;
using ServiceStack.Text.Support;

namespace ServiceStack.Redis.Tests.Benchmarks
{
	[TestFixture]
	public class DoubleSerializationBenchmarks
	{
		const int times = 100000;

		public void ResetGC()
		{
			GC.Collect();
			GC.WaitForPendingFinalizers();
			GC.Collect();			
		}

		[Test]
		public void Compare_double_serializers()
		{
			var initalVal = 0.3333333333333333d;

			var results = new string[times];

			ResetGC();
			var sw = Stopwatch.StartNew();

			for (var i = 0; i < times; i++)
			{
				results[i] = (initalVal + i).ToString();
			}

			Console.WriteLine("double.ToString(): Completed in ms: " + sw.ElapsedMilliseconds);
			//PrintLastValues(results, 100);

			ResetGC();
			sw = Stopwatch.StartNew();

			for (var i = 0; i < times; i++)
			{
				results[i] = (initalVal + i).ToString("r");
			}

			Console.WriteLine("double.ToString('r') completed in ms: " + sw.ElapsedMilliseconds);
			//PrintLastValues(results, 100);

			//Default
			ResetGC();
			sw = Stopwatch.StartNew();

			for (var i = 0; i < times; i++)
			{
				results[i] = DoubleConverter.ToExactString(initalVal + i);
			}

			Console.WriteLine("DoubleConverter.ToExactString(): Completed in ms: " + sw.ElapsedMilliseconds);
			//PrintLastValues(results, 100);

			//What #XBOX uses
			ResetGC();
			sw = Stopwatch.StartNew();

			for (var i = 0; i < times; i++)
			{
				results[i] = BitConverter.ToString(BitConverter.GetBytes(initalVal + i));
			}

			Console.WriteLine("BitConverter.ToString() completed in ms: " + sw.ElapsedMilliseconds);
			//PrintLastValues(results, 100); 


			//What Booksleeve uses
			ResetGC();
			sw = Stopwatch.StartNew();

			for (var i = 0; i < times; i++)
			{
				results[i] = (initalVal + i).ToString("G", CultureInfo.InvariantCulture);
			}

			Console.WriteLine("double.ToString('G') completed in ms: " + sw.ElapsedMilliseconds);
			//PrintLastValues(results, 100); 
		}

		private static void PrintLastValues(string[] results, int count)
		{
			var sb = new StringBuilder();
			for (int i = times - 1; i >= (times - count); i--)
				sb.AppendLine(results[i]);
			Console.WriteLine("Last {0} values: ".Fmt(count));
			Console.WriteLine(sb);
		}
	}
}