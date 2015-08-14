using System.Collections.Generic;
using System.Threading;

namespace ServiceStack.Redis
{
    public static class RedisStats
    {
        public static long TotalFailovers
        {
            get { return Interlocked.Read(ref RedisState.TotalFailovers); }
        }

        public static long TotalDeactivatedClients
        {
            get { return Interlocked.Read(ref RedisState.TotalDeactivatedClients); }
        }

        public static long TotalFailedSentinelWorkers
        {
            get { return Interlocked.Read(ref RedisState.TotalFailedSentinelWorkers); }
        }

        public static long TotalForcedMasterFailovers
        {
            get { return Interlocked.Read(ref RedisState.TotalForcedMasterFailovers); }
        }

        public static long TotalInvalidMasters
        {
            get { return Interlocked.Read(ref RedisState.TotalInvalidMasters); }
        }

        public static long TotalNoMastersFound
        {
            get { return Interlocked.Read(ref RedisState.TotalNoMastersFound); }
        }

        public static long TotalClientsCreated
        {
            get { return Interlocked.Read(ref RedisState.TotalClientsCreated); }
        }

        public static long TotalClientsCreatedOutsidePool
        {
            get { return Interlocked.Read(ref RedisState.TotalClientsCreatedOutsidePool); }
        }

        public static long TotalSubjectiveServersDown
        {
            get { return Interlocked.Read(ref RedisState.TotalSubjectiveServersDown); }
        }

        public static long TotalObjectiveServersDown
        {
            get { return Interlocked.Read(ref RedisState.TotalObjectiveServersDown); }
        }

        public static long TotalRetryCount
        {
            get { return Interlocked.Read(ref RedisState.TotalRetryCount); }
        }

        public static long TotalRetrySuccess
        {
            get { return Interlocked.Read(ref RedisState.TotalRetrySuccess); }
        }

        public static long TotalRetryTimedout
        {
            get { return Interlocked.Read(ref RedisState.TotalRetryTimedout); }
        }

        public static void Reset()
        {
            Interlocked.Exchange(ref RedisState.TotalFailovers, 0);
            Interlocked.Exchange(ref RedisState.TotalDeactivatedClients, 0);
            Interlocked.Exchange(ref RedisState.TotalFailedSentinelWorkers, 0);
            Interlocked.Exchange(ref RedisState.TotalForcedMasterFailovers, 0);
            Interlocked.Exchange(ref RedisState.TotalInvalidMasters, 0);
            Interlocked.Exchange(ref RedisState.TotalNoMastersFound, 0);
            Interlocked.Exchange(ref RedisState.TotalClientsCreated, 0);
            Interlocked.Exchange(ref RedisState.TotalClientsCreatedOutsidePool, 0);
            Interlocked.Exchange(ref RedisState.TotalSubjectiveServersDown, 0);
            Interlocked.Exchange(ref RedisState.TotalObjectiveServersDown, 0);
            Interlocked.Exchange(ref RedisState.TotalRetryCount, 0);
            Interlocked.Exchange(ref RedisState.TotalRetrySuccess, 0);
            Interlocked.Exchange(ref RedisState.TotalRetryTimedout, 0);
        }

        public static Dictionary<string, long> ToDictionary()
        {
            return new Dictionary<string, long> 
            {
                {"TotalFailovers", TotalFailovers},
                {"TotalDeactivatedClients", TotalDeactivatedClients},
                {"TotalFailedSentinelWorkers", TotalFailedSentinelWorkers},
                {"TotalForcedMasterFailovers", TotalForcedMasterFailovers},
                {"TotalInvalidMasters", TotalInvalidMasters},
                {"TotalNoMastersFound", TotalNoMastersFound},
                {"TotalClientsCreated", TotalClientsCreated},
                {"TotalClientsCreatedOutsidePool", TotalClientsCreatedOutsidePool},
                {"TotalSubjectiveServersDown", TotalSubjectiveServersDown},
                {"TotalObjectiveServersDown", TotalObjectiveServersDown},
                {"TotalPendingDeactivatedClients", RedisState.DeactivatedClients.Count },
                {"TotalRetryCount", TotalRetryCount },
                {"TotalRetrySuccess", TotalRetrySuccess },
                {"TotalRetryTimedout", TotalRetryTimedout },
            };
        }
    }
}