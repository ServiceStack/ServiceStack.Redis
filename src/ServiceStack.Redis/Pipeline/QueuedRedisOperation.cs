using System;
using System.Collections.Generic;
using System.Text;
using ServiceStack.Logging;

namespace ServiceStack.Redis.Pipeline
{
    internal class QueuedRedisOperation
    {
        protected static readonly ILog Log = LogManager.GetLogger(typeof(QueuedRedisOperation));

        public Action VoidReadCommand { get; set; }
        public Func<int> IntReadCommand { get; set; }
        public Func<long> LongReadCommand { get; set; }
        public Func<bool> BoolReadCommand { get; set; }
        public Func<byte[]> BytesReadCommand { get; set; }
        public Func<byte[][]> MultiBytesReadCommand { get; set; }
        public Func<string> StringReadCommand { get; set; }
        public Func<List<string>> MultiStringReadCommand { get; set; }
        public Func<Dictionary<string, string>> DictionaryStringReadCommand { get; set; }
        public Func<double> DoubleReadCommand { get; set; }
        public Func<RedisData> RedisDataReadCommand { get; set; }

        public Action OnSuccessVoidCallback { get; set; }
        public Action<int> OnSuccessIntCallback { get; set; }
        public Action<long> OnSuccessLongCallback { get; set; }
        public Action<bool> OnSuccessBoolCallback { get; set; }
        public Action<byte[]> OnSuccessBytesCallback { get; set; }
        public Action<byte[][]> OnSuccessMultiBytesCallback { get; set; }
        public Action<string> OnSuccessStringCallback { get; set; }
        public Action<List<string>> OnSuccessMultiStringCallback { get; set; }
        public Action<Dictionary<string, string>> OnSuccessDictionaryStringCallback { get; set; }
        public Action<RedisData> OnSuccessRedisDataCallback { get; set; }
        public Action<RedisText> OnSuccessRedisTextCallback { get; set; }
        public Action<double> OnSuccessDoubleCallback { get; set; }

        public Action<string> OnSuccessTypeCallback { get; set; }
        public Action<List<string>> OnSuccessMultiTypeCallback { get; set; }

        public Action<Exception> OnErrorCallback { get; set; }

        public virtual void Execute(IRedisClient client)
        {

        }

        public void ProcessResult()
        {
            try
            {
                if (VoidReadCommand != null)
                {
                    VoidReadCommand();
                    if (OnSuccessVoidCallback != null)
                    {
                        OnSuccessVoidCallback();
                    }
                }
                else if (IntReadCommand != null)
                {
                    var result = IntReadCommand();
                    if (OnSuccessIntCallback != null)
                    {
                        OnSuccessIntCallback(result);
                    }
                    if (OnSuccessLongCallback != null)
                    {
                        OnSuccessLongCallback(result);
                    }
                    if (OnSuccessBoolCallback != null)
                    {
                        var success = result == RedisNativeClient.Success;
                        OnSuccessBoolCallback(success);
                    }
                    if (OnSuccessVoidCallback != null)
                    {
                        OnSuccessVoidCallback();
                    }
                }
                else if (LongReadCommand != null)
                {
                    var result = LongReadCommand();
                    if (OnSuccessIntCallback != null)
                    {
                        OnSuccessIntCallback((int)result);
                    }
                    if (OnSuccessLongCallback != null)
                    {
                        OnSuccessLongCallback(result);
                    }
                    if (OnSuccessBoolCallback != null)
                    {
                        var success = result == RedisNativeClient.Success;
                        OnSuccessBoolCallback(success);
                    }
                    if (OnSuccessVoidCallback != null)
                    {
                        OnSuccessVoidCallback();
                    }
                }
                else if (DoubleReadCommand != null)
                {
                    var result = DoubleReadCommand();
                    if (OnSuccessDoubleCallback != null)
                    {
                        OnSuccessDoubleCallback(result);
                    }
                }
                else if (BytesReadCommand != null)
                {
                    var result = BytesReadCommand();
                    if (result != null && result.Length == 0)
                        result = null;

                    if (OnSuccessBytesCallback != null)
                    {
                        OnSuccessBytesCallback(result);
                    }
                    if (OnSuccessStringCallback != null)
                    {
                        OnSuccessStringCallback(result != null ? Encoding.UTF8.GetString(result) : null);
                    }
                    if (OnSuccessTypeCallback != null)
                    {
                        OnSuccessTypeCallback(result != null ? Encoding.UTF8.GetString(result) : null);
                    }
                    if (OnSuccessIntCallback != null)
                    {
                        OnSuccessIntCallback(result != null ? int.Parse(Encoding.UTF8.GetString(result)) : 0);
                    }
                }
                else if (StringReadCommand != null)
                {
                    var result = StringReadCommand();
                    if (OnSuccessStringCallback != null)
                    {
                        OnSuccessStringCallback(result);
                    }
                    if (OnSuccessTypeCallback != null)
                    {
                        OnSuccessTypeCallback(result);
                    }
                }
                else if (MultiBytesReadCommand != null)
                {
                    var result = MultiBytesReadCommand();
                    if (OnSuccessMultiBytesCallback != null)
                    {
                        OnSuccessMultiBytesCallback(result);
                    }
                    if (OnSuccessMultiStringCallback != null)
                    {
                        OnSuccessMultiStringCallback(result != null ? result.ToStringList() : null);
                    }
                    if (OnSuccessMultiTypeCallback != null)
                    {
                        OnSuccessMultiTypeCallback(result.ToStringList());
                    }
                    if (OnSuccessDictionaryStringCallback != null)
                    {
                        OnSuccessDictionaryStringCallback(result.ToStringDictionary());
                    }
                }
                else if (MultiStringReadCommand != null)
                {
                    var result = MultiStringReadCommand();
                    if (OnSuccessMultiStringCallback != null)
                    {
                        OnSuccessMultiStringCallback(result);
                    }
                }
                else if (RedisDataReadCommand != null)
                {
                    var data = RedisDataReadCommand();
                    if (OnSuccessRedisTextCallback != null)
                    {
                        OnSuccessRedisTextCallback(data.ToRedisText());
                    }
                    if (OnSuccessRedisDataCallback != null)
                    {
                        OnSuccessRedisDataCallback(data);
                    }
                }
            }
            catch (Exception ex)
            {
                Log.Error(ex);

                if (OnErrorCallback != null)
                {
                    OnErrorCallback(ex);
                }
                else
                {
                    throw;
                }
            }
        }

    }
}