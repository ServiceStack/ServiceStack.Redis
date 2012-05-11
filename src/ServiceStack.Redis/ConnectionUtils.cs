//using System;
//using System.Collections.Generic;
//using System.Collections.Specialized;
//using System.IO;
//using System.Linq;
//using System.Text;

//namespace ServiceStack.Redis
//{
//    /// <summary>
//    /// Provides utility methods for managing connections to multiple (master/slave) redis servers (with the same
//    /// information - not sharding).
//    /// </summary>
//    public static class ConnectionUtils
//    {
//        /// <summary>
//        /// Inspect the provided configration, and connect to the available servers to report which server is the preferred/active node.
//        /// </summary>
//        public static string SelectConfiguration(string configuration, out string[] availableEndpoints, TextWriter log = null)
//        {
//            string selected;
//            using (SelectAndCreateConnection(configuration, log, out selected, out availableEndpoints, false)) { }
//            return selected;
//        }
//        /// <summary>
//        /// Inspect the provided configration, and connect to the preferred/active node after checking what nodes are available.
//        /// </summary>
//        public static RedisConnection Connect(string configuration, TextWriter log = null)
//        {
//            string selectedConfiguration;
//            string[] availableEndpoints;
//            return SelectAndCreateConnection(configuration, log, out selectedConfiguration, out availableEndpoints, true);
//        }

//        /// <summary>
//        /// Subscribe to perform some operation when a change to the preferred/active node is broadcast.
//        /// </summary>
//        public static void SubscribeToMasterSwitch(RedisSubscriberConnection connection, Action<string> handler)
//        {
//            if (connection == null) throw new ArgumentNullException("connection");
//            if (handler == null) throw new ArgumentNullException("handler");

//            connection.Subscribe(RedisMasterChangedChannel, (channel, message) => handler(Encoding.UTF8.GetString(message)));
//        }
//        /// <summary>
//        /// Using the configuration available, and after checking which nodes are available, switch the master node and broadcast this change.
//        /// </summary>
//        public static void SwitchMaster(string configuration, string newMaster, TextWriter log = null)
//        {
//            string newConfig;
//            string[] availableEndpoints;

//            SelectAndCreateConnection(configuration, log, out newConfig, out availableEndpoints, false, newMaster);
//        }

//        const string RedisMasterChangedChannel = "__Booksleeve_MasterChanged", TieBreakerKey = "__Booksleeve_TieBreak";

//        /// <summary>
//        /// Prompt all clients to reconnect.
//        /// </summary>
//        public static void BroadcastReconnectMessage(RedisConnection connection)
//        {
//            if (connection == null) throw new ArgumentNullException("connection");

//            connection.Wait(connection.Publish(RedisMasterChangedChannel, "*"));
//        }
//        private static RedisConnection SelectWithTieBreak(TextWriter log, List<RedisConnection> nodes, Dictionary<string, int> tiebreakers)
//        {
//            if (nodes.Count == 0) return null;
//            if (nodes.Count == 1) return nodes[0];
//            Func<string, int> valueOrDefault = key =>
//            {
//                int tmp;
//                if (!tiebreakers.TryGetValue(key, out tmp)) tmp = 0;
//                return tmp;
//            };
//            var tuples = (from node in nodes
//                          let key = node.Host + ":" + node.Port
//                          let count = valueOrDefault(key)
//                          select new { Node = node, Key = key, Count = count }).ToList();

//            // check for uncontested scenario
//            int contenderCount = tuples.Count(x => x.Count > 0);
//            switch (contenderCount)
//            {
//                case 0:
//                    log.WriteLine("No tie-break contenders; selecting arbitrary node");
//                    return tuples[0].Node;
//                case 1:
//                    log.WriteLine("Unaminous tie-break winner");
//                    return tuples.Single(x => x.Count > 0).Node;
//            }

//            // contested
//            int maxCount = tuples.Max(x => x.Count);
//            var competing = tuples.Where(x => x.Count == maxCount).ToList();

//            switch (competing.Count)
//            {
//                case 0:
//                    return null; // impossible, but never rely on the impossible not happening ;p
//                case 1:
//                    log.WriteLine("Contested, but clear, tie-break winner");
//                    break;
//                default:
//                    log.WriteLine("Contested and ambiguous tie-break; selecting arbitrary node");
//                    break;
//            }
//            return competing[0].Node;
//        }

//        private static string[] GetConfigurationOptions(string configuration, out int syncTimeout, out bool allowAdmin)
//        {
//            syncTimeout = 1000;
//            allowAdmin = false;

//            // break it down by commas
//            var arr = configuration.Split(',');
//            var options = new List<string>();
//            foreach (var option in arr)
//            {
//                var trimmed = option.Trim();

//                if (trimmed.IsNullOrWhiteSpace() || options.Contains(trimmed)) continue;

//                // check for special tokens
//                int idx = trimmed.IndexOf('=');
//                if (idx > 0)
//                {
//                    if (option.StartsWith(SyncTimeoutPrefix))
//                    {
//                        int tmp;
//                        if (int.TryParse(option.Substring(idx + 1), out tmp)) syncTimeout = tmp;
//                        continue;
//                    }
//                    if (option.StartsWith(AllowAdminPrefix))
//                    {
//                        bool tmp;
//                        if (bool.TryParse(option.Substring(idx + 1), out tmp)) allowAdmin = tmp;
//                        continue;
//                    }
//                }

//                options.Add(trimmed);
//            }
//            return options.ToArray();
//        }

//        internal const string AllowAdminPrefix = "allowAdmin=", SyncTimeoutPrefix = "syncTimeout=";
//        private static RedisConnection SelectAndCreateConnection(string configuration, TextWriter log, out string selectedConfiguration, out string[] availableEndpoints, bool autoMaster, string newMaster = null)
//        {
//            int syncTimeout;
//            bool allowAdmin;
//            if (log == null) log = new StringWriter();
//            var arr = GetConfigurationOptions(configuration, out syncTimeout, out allowAdmin);
//            if (!newMaster.IsNullOrWhiteSpace()) allowAdmin = true; // need this to diddle the slave/master config

//            log.WriteLine("{0} unique nodes specified", arr.Length);
//            log.WriteLine("sync timeout: {0}ms, admin commands: {1}", syncTimeout,
//                          allowAdmin ? "enabled" : "disabled");
//            if (arr.Length == 0)
//            {
//                log.WriteLine("No nodes to consider");
//                selectedConfiguration = null;
//                availableEndpoints = new string[0];
//                return null;
//            }
//            var connections = new List<RedisConnection>(arr.Length);
//            RedisConnection preferred = null;

//            try
//            {
//                var infos = new List<Task<string>>(arr.Length);
//                var tiebreakers = new List<Task<string>>(arr.Length);
//                foreach (var option in arr)
//                {
//                    if (option.IsNullOrWhiteSpace()) continue;

//                    RedisConnection conn = null;
//                    try
//                    {

//                        var parts = option.Split(':');
//                        if (parts.Length == 0) continue;

//                        string host = parts[0].Trim();
//                        int port = 6379, tmp;
//                        if (parts.Length > 1 && int.TryParse(parts[1].Trim(), out tmp)) port = tmp;
//                        conn = new RedisConnection(host, port, syncTimeout: syncTimeout, allowAdmin: allowAdmin);

//                        log.WriteLine("Opening connection to {0}:{1}...", host, port);
//                        conn.Open();
//                        var info = conn.GetInfo();
//                        var tiebreak = conn.Strings.GetString(0, TieBreakerKey);
//                        connections.Add(conn);
//                        infos.Add(info);
//                        tiebreakers.Add(tiebreak);
//                    }
//                    catch (Exception ex)
//                    {
//                        if (conn == null)
//                        {
//                            log.WriteLine("Error parsing option \"{0}\": {1}", option, ex.Message);
//                        }
//                        else
//                        {
//                            log.WriteLine("Error connecting: {0}", ex.Message);
//                        }
//                    }
//                }
//                List<RedisConnection> masters = new List<RedisConnection>(), slaves = new List<RedisConnection>();
//                var breakerScores = new Dictionary<string, int>();
//                foreach (var tiebreak in tiebreakers)
//                {
//                    try
//                    {
//                        if (tiebreak.Wait(syncTimeout))
//                        {
//                            string key = tiebreak.Result;
//                            if (key.IsNullOrWhiteSpace()) continue;
//                            int score;
//                            if (breakerScores.TryGetValue(key, out score)) breakerScores[key] = score + 1;
//                            else breakerScores.Add(key, 1);
//                        }
//                    }
//                    catch { /* if a node is down, that's fine too */ }
//                }
//                // check for tie-breakers (i.e. when we store which is the master)
//                switch (breakerScores.Count)
//                {
//                    case 0:
//                        log.WriteLine("No tie-breakers found");
//                        break;
//                    case 1:
//                        log.WriteLine("Tie-breaker is unanimous: {0}", breakerScores.Keys.Single());
//                        break;
//                    default:
//                        log.WriteLine("Ambiguous tie-breakers:");
//                        foreach (var kvp in breakerScores.OrderByDescending(x => x.Value))
//                        {
//                            log.WriteLine("\t{0}: {1}", kvp.Key, kvp.Value);
//                        }
//                        break;
//                }

//                for (int i = 0; i < connections.Count; i++)
//                {
//                    log.WriteLine("Reading configuration from {0}:{1}...", connections[i].Host, connections[i].Port);
//                    try
//                    {
//                        if (!infos[i].Wait(syncTimeout))
//                        {
//                            log.WriteLine("\tTimeout fetching INFO");
//                            continue;
//                        }
//                        var infoPairs = new StringDictionary();
//                        using (var sr = new StringReader(infos[i].Result))
//                        {
//                            string line;
//                            while ((line = sr.ReadLine()) != null)
//                            {
//                                int idx = line.IndexOf(':');
//                                if (idx < 0) continue;
//                                string key = line.Substring(0, idx).Trim(),
//                                       value = line.Substring(idx + 1, line.Length - (idx + 1)).Trim();
//                                infoPairs[key] = value;
//                            }
//                        }
//                        string role = infoPairs["role"];
//                        switch (role)
//                        {
//                            case "slave":
//                                log.WriteLine("\tServer is SLAVE of {0}:{1}",
//                                          infoPairs["master_host"], infoPairs["master_port"]);
//                                log.Write("\tLink is {0}, seen {1} seconds ago",
//                                                 infoPairs["master_link_status"], infoPairs["master_last_io_seconds_ago"]);
//                                if (infoPairs["master_sync_in_progress"] == "1") log.Write(" (sync is in progress)");
//                                log.WriteLine();
//                                slaves.Add(connections[i]);
//                                break;
//                            case "master":
//                                log.WriteLine("\tServer is MASTER, with {0} slaves", infoPairs["connected_slaves"]);
//                                masters.Add(connections[i]);
//                                break;
//                            default:
//                                log.WriteLine("\tUnknown role: {0}", role);
//                                break;
//                        }
//                        string tmp = infoPairs["connected_clients"];
//                        int clientCount, channelCount, patternCount;
//                        if (tmp.IsNullOrWhiteSpace() || !int.TryParse(tmp, out clientCount)) clientCount = -1;
//                        tmp = infoPairs["pubsub_channels"];
//                        if (tmp.IsNullOrWhiteSpace(tmp) || !int.TryParse(tmp, out channelCount)) channelCount = -1;
//                        tmp = infoPairs["pubsub_patterns"];
//                        if (tmp.IsNullOrWhiteSpace(tmp) || !int.TryParse(tmp, out patternCount)) patternCount = -1;
//                        log.WriteLine("\tClients: {0}; channels: {1}; patterns: {2}", clientCount, channelCount, patternCount);
//                    }
//                    catch (Exception ex)
//                    {
//                        log.WriteLine("\tError reading INFO results: {0}", ex.Message);
//                    }
//                }

//                if (newMaster == null)
//                {
//                    switch (masters.Count)
//                    {
//                        case 0:
//                            switch (slaves.Count)
//                            {
//                                case 0:
//                                    log.WriteLine("No masters or slaves found");
//                                    break;
//                                case 1:
//                                    log.WriteLine("No masters found; selecting single slave");
//                                    preferred = slaves[0];
//                                    break;
//                                default:
//                                    log.WriteLine("No masters found; considering {0} slaves...", slaves.Count);
//                                    preferred = SelectWithTieBreak(log, slaves, breakerScores);
//                                    break;
//                            }
//                            if (preferred != null)
//                            {
//                                if (autoMaster)
//                                {
//                                    //LogException("Promoting redis SLAVE to MASTER");
//                                    log.WriteLine("Promoting slave to master...");
//                                    if (allowAdmin)
//                                    { // can do on this connection
//                                        preferred.Wait(preferred.Server.MakeMaster());
//                                    }
//                                    else
//                                    { // need an admin connection for this
//                                        using (var adminPreferred = new RedisConnection(preferred.Host, preferred.Port, allowAdmin: true, syncTimeout: syncTimeout))
//                                        {
//                                            adminPreferred.Open();
//                                            adminPreferred.Wait(adminPreferred.Server.MakeMaster());
//                                        }
//                                    }
//                                }
//                                else
//                                {
//                                    log.WriteLine("Slave should be promoted to master (but not done yet)...");
//                                }
//                            }
//                            break;
//                        case 1:
//                            log.WriteLine("One master found; selecting");
//                            preferred = masters[0];
//                            break;
//                        default:
//                            log.WriteLine("Considering {0} masters...", masters.Count);
//                            preferred = SelectWithTieBreak(log, masters, breakerScores);
//                            break;
//                    }


//                }
//                else
//                { // we have been instructed to change master server
//                    preferred = masters.Concat(slaves).FirstOrDefault(conn => (conn.Host + ":" + conn.Port) == newMaster);
//                    if (preferred == null)
//                    {
//                        log.WriteLine("Selected new master not available: {0}", newMaster);
//                    }
//                    else
//                    {
//                        int errorCount = 0;
//                        try
//                        {
//                            log.WriteLine("Promoting to master: {0}:{1}...", preferred.Host, preferred.Port);
//                            preferred.Wait(preferred.Server.MakeMaster());
//                            preferred.Strings.Set(0, TieBreakerKey, newMaster);
//                            preferred.Wait(preferred.Publish(RedisMasterChangedChannel, newMaster));
//                        }
//                        catch (Exception ex)
//                        {
//                            log.WriteLine("\t{0}", ex.Message);
//                            errorCount++;
//                        }

//                        if (errorCount == 0) // only make slaves if the master was happy
//                        {
//                            foreach (var conn in masters.Concat(slaves))
//                            {
//                                if (conn == preferred) continue; // can't make self a slave!

//                                try
//                                {
//                                    log.WriteLine("Enslaving: {0}:{1}...", conn.Host, conn.Port);
//                                    // set the tie-breaker **first** in case of problems
//                                    conn.Strings.Set(0, TieBreakerKey, newMaster);
//                                    // and broadcast to anyone who thinks this is the master
//                                    conn.Publish(RedisMasterChangedChannel, newMaster);
//                                    // now make it a slave
//                                    conn.Wait(conn.Server.MakeSlave(preferred.Host, preferred.Port));
//                                }
//                                catch (Exception ex)
//                                {
//                                    log.WriteLine("\t{0}", ex.Message);
//                                    errorCount++;
//                                }
//                            }
//                        }
//                        if (errorCount != 0)
//                        {
//                            log.WriteLine("Things didn't go smoothly; CHECK WHAT HAPPENED!");
//                        }

//                        // want the connection disposed etc
//                        preferred = null;
//                    }
//                }

//                if (preferred == null)
//                {
//                    selectedConfiguration = null;
//                }
//                else
//                {
//                    selectedConfiguration = preferred.Host + ":" + preferred.Port;
//                    log.WriteLine("Selected server {0}", selectedConfiguration);
//                }

//                availableEndpoints = (from conn in masters.Concat(slaves)
//                                      select conn.Host + ":" + conn.Port).ToArray();
//                return preferred;
//            }
//            finally
//            {
//                foreach (var conn in connections)
//                {
//                    if (conn != null && conn != preferred) try { conn.Dispose(); }
//                        catch { }
//                }
//            }
//        }

//    }

//    public static class ConnectionUtilsExtensions
//    {
//        public static bool IsNullOrWhiteSpace(this string str)
//        {
//            return str == null || str.Trim().Length == 0;
//        }
//    }
//}