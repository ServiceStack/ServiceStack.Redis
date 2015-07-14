using System.Collections.Generic;
using System.Threading;

namespace ServiceStack.Redis
{
    public static class RedisStats
    {
        public static long TotalFailovers
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalFailovers, 0, 0); }
        }

        public static long TotalDeactivatedClients
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalDeactivatedClients, 0, 0); }
        }

        public static long TotalFailedSentinelWorkers
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalFailedSentinelWorkers, 0, 0); }
        }

        public static long TotalInvalidMasters
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalInvalidMasters, 0, 0); }
        }

        public static long TotalNoMastersFound
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalNoMastersFound, 0, 0); }
        }

        public static long TotalClientsCreated
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalClientsCreated, 0, 0); }
        }

        public static long TotalSubjectiveServersDown
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalSubjectiveServersDown, 0, 0); }
        }

        public static long TotalObjectiveServersDown
        {
            get { return Interlocked.CompareExchange(ref RedisState.TotalObjectiveServersDown, 0, 0); }
        }

        public static Dictionary<string, long> ToDictionary()
        {
            return new Dictionary<string, long> 
            {
                {"TotalFailovers", TotalFailovers},
                {"TotalDeactivatedClients", TotalDeactivatedClients},
                {"TotalFailedSentinelWorkers", TotalFailedSentinelWorkers},
                {"TotalInvalidMasters", TotalInvalidMasters},
                {"TotalNoMastersFound", TotalNoMastersFound},
                {"TotalClientsCreated", TotalClientsCreated},
                {"TotalSubjectiveServersDown", TotalSubjectiveServersDown},
                {"TotalObjectiveServersDown", TotalObjectiveServersDown},
            };
        }
    }
}