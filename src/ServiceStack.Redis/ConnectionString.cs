using System;
using System.Collections.Generic;
using System.Linq;

namespace ServiceStack.Redis
{
    public class ConnectionString
    {
        private Dictionary<string, string> _parameters =
            new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase);

        public ConnectionString(string connectionString)
        {
            Parse(connectionString);
        }

        public string Host
        {
            get
            {
                return _parameters.ContainsKey("host") ? _parameters["host"] : RedisClient.DefaultHost;
            }
        }

        public int Port
        {
            get
            {
                if (!_parameters.ContainsKey("port"))
                    return RedisClient.DefaultPort;

                int port;
                if (!int.TryParse(_parameters["port"], out port))
                    return RedisClient.DefaultPort;

                return port;
            }
        }

        public string Password
        {
            get
            {
                return _parameters.ContainsKey("password") ? _parameters["password"] : null;
            }
        }

        public int Db
        {
            get
            {
                if (!_parameters.ContainsKey("db"))
                    return RedisClient.DefaultDb;

                int db;
                if (!int.TryParse(_parameters["db"], out db))
                    return RedisClient.DefaultDb;

                return db;
            }
        }

        private void Parse(string connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException("connectionString");

            _parameters = connectionString.Split(new[] {';'}, StringSplitOptions.RemoveEmptyEntries)
                                        .Select(x => x.Split(new[] {'='}, StringSplitOptions.RemoveEmptyEntries))
                                        .Where(x => x.Length == 2)
                                        .ToDictionary(x => x[0].ToLower(), x => x[1]);
        }
    }
}