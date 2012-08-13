using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using NUnit.Framework;
using ServiceStack.Common.Extensions;
using ServiceStack.Text;

namespace ServiceStack.Redis.Tests
{
	[TestFixture, Category("Integration")]
	public class RedisPubSubTests
		: RedisClientTestsBase
	{
		[Test]
		public void Can_Subscribe_and_Publish_single_message()
		{
			const string channelName = "CHANNEL";
			const string message = "Hello, World!";

			Redis.IncrementValue("CanUseNormalClient");

			using (var subscription = Redis.CreateSubscription())
			{
				subscription.OnSubscribe = channel =>
				{
					Log("Subscribed to '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelName));
				};
				subscription.OnUnSubscribe = channel =>
				{
					Log("UnSubscribed from '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelName));
				};
				subscription.OnMessage = (channel, msg) =>
				{
					Log("Received '{0}' from channel '{1}'", msg, channel);
					Assert.That(channel, Is.EqualTo(channelName));
					Assert.That(msg, Is.EqualTo(message));
					subscription.UnSubscribeFromAllChannels();
				};

				ThreadPool.QueueUserWorkItem(x =>
				{
					using (var redisClient = CreateRedisClient())
					{
						Log("Publishing '{0}' to '{1}'", message, channelName);
						redisClient.PublishMessage(channelName, message);
					}
				});

				Log("Start Listening On " + channelName);
				subscription.SubscribeToChannels(channelName); //blocking
			}

			Log("Using as normal client again...");
			Redis.IncrementValue("CanUseNormalClient");
			Assert.That(Redis.Get<int>("CanUseNormalClient"), Is.EqualTo(2));
		}

		[Test]
		public void Can_Subscribe_and_Publish_multiple_message()
		{
			const string channelName = "CHANNEL";
			const string messagePrefix = "MESSAGE ";
			const int publishMessageCount = 5;
			var messagesReceived = 0;

			Redis.IncrementValue("CanUseNormalClient");

			using (var subscription = Redis.CreateSubscription())
			{
				subscription.OnSubscribe = channel =>
				{
					Log("Subscribed to '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelName));
				};
				subscription.OnUnSubscribe = channel =>
				{
					Log("UnSubscribed from '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelName));
				};
				subscription.OnMessage = (channel, msg) =>
				{
					Log("Received '{0}' from channel '{1}'", msg, channel);
					Assert.That(channel, Is.EqualTo(channelName));
					Assert.That(msg, Is.EqualTo(messagePrefix + messagesReceived++));

					if (messagesReceived == publishMessageCount)
					{
						subscription.UnSubscribeFromAllChannels();
					}
				};

				ThreadPool.QueueUserWorkItem(x =>
				{
					using (var redisClient = CreateRedisClient())
					{
						for (var i = 0; i < publishMessageCount; i++)
						{
							var message = messagePrefix + i;
							Log("Publishing '{0}' to '{1}'", message, channelName);
							redisClient.PublishMessage(channelName, message);
						}
					}
				});

				Log("Start Listening On");
				subscription.SubscribeToChannels(channelName); //blocking
			}

			Log("Using as normal client again...");
			Redis.IncrementValue("CanUseNormalClient");
			Assert.That(Redis.Get<int>("CanUseNormalClient"), Is.EqualTo(2));

			Assert.That(messagesReceived, Is.EqualTo(publishMessageCount));
		}

		[Test]
		public void Can_Subscribe_and_Publish_message_to_multiple_channels()
		{
			const string channelPrefix = "CHANNEL ";
			const string message = "MESSAGE";
			const int publishChannelCount = 5;

			var channels = new List<string>();
			publishChannelCount.Times(i => channels.Add(channelPrefix + i));

			var messagesReceived = 0;
			var channelsSubscribed = 0;
			var channelsUnSubscribed = 0;

			Redis.IncrementValue("CanUseNormalClient");

			using (var subscription = Redis.CreateSubscription())
			{
				subscription.OnSubscribe = channel =>
				{
					Log("Subscribed to '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelPrefix + channelsSubscribed++));
				};
				subscription.OnUnSubscribe = channel =>
				{
					Log("UnSubscribed from '{0}'", channel);
					Assert.That(channel, Is.EqualTo(channelPrefix + channelsUnSubscribed++));
				};
				subscription.OnMessage = (channel, msg) =>
				{
					Log("Received '{0}' from channel '{1}'", msg, channel);
					Assert.That(channel, Is.EqualTo(channelPrefix + messagesReceived++));
					Assert.That(msg, Is.EqualTo(message));

					subscription.UnSubscribeFromChannels(channel);
				};

				ThreadPool.QueueUserWorkItem(x =>
				{
					using (var redisClient = CreateRedisClient())
					{
						foreach (var channel in channels)
						{
							Log("Publishing '{0}' to '{1}'", message, channel);
							redisClient.PublishMessage(channel, message);
						}
					}
				});

				Log("Start Listening On");
				subscription.SubscribeToChannels(channels.ToArray()); //blocking
			}

			Log("Using as normal client again...");
			Redis.IncrementValue("CanUseNormalClient");
			Assert.That(Redis.Get<int>("CanUseNormalClient"), Is.EqualTo(2));

			Assert.That(messagesReceived, Is.EqualTo(publishChannelCount));
			Assert.That(channelsSubscribed, Is.EqualTo(publishChannelCount));
			Assert.That(channelsUnSubscribed, Is.EqualTo(publishChannelCount));
		}

		[Test]
		public void Can_Subscribe_to_channel_pattern()
		{
			int msgs = 0;
			using (var subscription = Redis.CreateSubscription())
			{
				subscription.OnMessage = (channel, msg) => {
					Debug.WriteLine(String.Format("{0}: {1}", channel, msg + msgs++));
					subscription.UnSubscribeFromChannelsMatching("CHANNEL1:TITLE*");
				};

				ThreadPool.QueueUserWorkItem(x =>
				{
					Thread.Sleep(100);
					using (var redisClient = CreateRedisClient())
					{
						Log("Publishing msg...");
						redisClient.Publish("CHANNEL1:TITLE1", "hello".ToUtf8Bytes());
					}
				});

				Log("Start Listening On");
				subscription.SubscribeToChannelsMatching("CHANNEL1:TITLE*");
			}
		}

	}
}