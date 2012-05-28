namespace ServiceStack.Redis.Messaging
{
    public static class WorkerStatus
    {
        public const int Disposed = -1;
        public const int Stopped = 0;
        public const int Stopping = 1;
        public const int Starting = 2;
        public const int Started = 3;

        //Control Commands
        public const string StopCommand = "STOP";
    }
}