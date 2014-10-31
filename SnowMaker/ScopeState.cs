using System.Threading.Tasks;
namespace SnowMaker
{
    class ScopeState
    {
        public readonly object IdGenerationLock = new object();
        public long LastId;
        public long HighestIdAvailableInBatch;

        public long NextBatchMin;
        public long NextBatchMax;
        public Task PrefetchTask;
    }
}