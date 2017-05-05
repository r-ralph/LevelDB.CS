namespace LevelDB
{
    public class WriteOptions
    {
        private bool _sync;
        private bool _snapshot;

        public bool Sync()
        {
            return _sync;
        }

        public WriteOptions Sync(bool sync)
        {
            _sync = sync;
            return this;
        }

        public bool Snapshot()
        {
            return _snapshot;
        }

        public WriteOptions Snapshot(bool snapshot)
        {
            _snapshot = snapshot;
            return this;
        }
    }
}