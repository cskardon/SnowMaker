using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;

namespace SnowMaker
{
    public class UniqueIdGenerator : IUniqueIdGenerator
    {
        readonly IOptimisticDataStore optimisticDataStore;

        //readonly IDictionary<string, ScopeState> states = new Dictionary<string, ScopeState>();
        readonly ConcurrentDictionary<string, ScopeState> states = new ConcurrentDictionary<string, ScopeState>();
        readonly object statesLock = new object();

        int batchSize = 100;
        int maxWriteAttempts = 25;
        int prefetchWhenLeft;
        byte prefetchWhen = 25; // %

        public UniqueIdGenerator(IOptimisticDataStore optimisticDataStore)
        {
            this.optimisticDataStore = optimisticDataStore;
            InitPrefetch();
        }

        public int BatchSize
        {
            get { return batchSize; }
            set
            {
                batchSize = value;
                InitPrefetch();
            }
        }

        public int MaxWriteAttempts
        {
            get { return maxWriteAttempts; }
            set
            {
                if (value < 1)
                    throw new ArgumentOutOfRangeException("value", maxWriteAttempts, "MaxWriteAttempts must be a positive number.");

                maxWriteAttempts = value;
            }
        }

        public byte PrefetchWhen
        {
            get { return prefetchWhen; }
            set
            {
                if (value > 100)
                    throw new ArgumentOutOfRangeException("value", maxWriteAttempts, "PrefetchWhen must be <= 100.");
                prefetchWhen = value;
                InitPrefetch();
            }
        }

        private void InitPrefetch()
        {
            prefetchWhenLeft = (int)(batchSize * prefetchWhen / 100.0);
        }

        public long NextId(string scopeName, int? batchSize = null, int? prefetchWhenLeft = null)
        {
            var prefetchWhenLeft_ = prefetchWhenLeft ?? this.prefetchWhenLeft;
            var batchSize_ = batchSize ?? this.batchSize;

            var state = GetScopeState(scopeName);

            lock (state.IdGenerationLock) {
                if (state.LastId == state.HighestIdAvailableInBatch)
                    FetchFromSyncStore(scopeName, state, batchSize_);
                else if (prefetchWhenLeft_ > 0 && (state.LastId + prefetchWhenLeft_) >= state.HighestIdAvailableInBatch)
                    PrefetchFromSyncStore(scopeName, state, batchSize_);

                return ++state.LastId;
                //return Interlocked.Increment(ref state.LastId);
            }
        }

        ScopeState GetScopeState(string scopeName)
        {
            return states.GetOrAdd(scopeName, s => new ScopeState());

            //return states.GetValue(
            //    scopeName,
            //    statesLock,
            //    () => new ScopeState());
        }

        private void PrefetchFromSyncStore(string scopeName, ScopeState state, int batchSize)
        {
            if (state.PrefetchTask != null)
                return;
            state.PrefetchTask = Task.Run(() => UpdateFromSyncStore(scopeName, state, batchSize));
        }

        private void FetchFromSyncStore(string scopeName, ScopeState state, int batchSize)
        {
            if (state.PrefetchTask != null) {
                try {
                    state.PrefetchTask.Wait();
                }
                finally {
                    state.PrefetchTask = null;
                }
            }
            else {
                UpdateFromSyncStore(scopeName, state, batchSize);
            }

            state.LastId = state.NextBatchMin;
            state.HighestIdAvailableInBatch = state.NextBatchMax;

            state.NextBatchMin = 0;
            state.NextBatchMax = 0;
        }

        void UpdateFromSyncStore(string scopeName, ScopeState state, int batchSize)
        {
            var writesAttempted = 0;

            while (writesAttempted < maxWriteAttempts) {
                var data = optimisticDataStore.GetData(scopeName);

                long nextId;
                if (!long.TryParse(data, out nextId))
                    throw new UniqueIdGenerationException(string.Format(
                       "The id seed returned from storage for scope '{0}' was corrupt, and could not be parsed as a long. The data returned was: {1}",
                       scopeName,
                       data));

                //state.LastId = nextId - 1;
                //state.HighestIdAvailableInBatch = nextId - 1 + batchSize;
                //var firstIdInNextBatch = state.HighestIdAvailableInBatch + 1;
                state.NextBatchMin = nextId - 1;
                state.NextBatchMax = nextId - 1 + batchSize;
                var firstIdInNextBatch = state.NextBatchMax + 1;

                if (optimisticDataStore.TryOptimisticWrite(scopeName, firstIdInNextBatch.ToString(CultureInfo.InvariantCulture)))
                    return;

                writesAttempted++;
            }

            throw new UniqueIdGenerationException(string.Format(
                "Failed to update the data store after {0} attempts. This likely represents too much contention against the store. Increase the batch size to a value more appropriate to your generation load.",
                writesAttempted));
        }
    }
}
