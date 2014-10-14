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

         public static string GetResult(this RedisText from)
         {
             return from.Text;
         }

         public static T GetResult<T>(this RedisText from)
         {
             return from.Text.FromJson<T>();
         }

         public static List<string> GetResults(this RedisText from)
         {
             return from.Children == null
                 ? new List<string>()
                 : from.Children.ConvertAll(x => x.Text);
         }

         public static List<T> GetResults<T>(this RedisText from)
         {
             return from.Children == null
                 ? new List<T>()
                 : from.Children.ConvertAll(x => x.Text.FromJson<T>());
         }
    }
}