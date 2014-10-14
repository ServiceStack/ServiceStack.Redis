using System.Collections.Generic;

namespace ServiceStack.Redis
{
    public static class RedisDataExtensions
    {
         public static RedisText ToRedisText(this RedisData data)
         {
             var to = new RedisText();

             if (data.Data != null)
                 to.Text = data.Data.FromUtf8Bytes();

             if (data.Children != null)
                 to.Children = data.Children.ConvertAll(x => x.ToRedisText());

             return to;
         }
    }
}