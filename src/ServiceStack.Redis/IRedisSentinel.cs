using System;
namespace ServiceStack.Redis
{
    public interface IRedisSentinel : IDisposable
    {
	// Obsolete
        IRedisClientsManager Setup();
	IRedisClientsManager Setup(int initialDb, int? poolSizeMultiplier, int? poolTimeOutSeconds);

        IRedisClientsManager Start();
	IRedisClientsManager Start(int initialDb, int? poolSizeMultiplier, int? poolTimeOutSeconds);
    }
}
