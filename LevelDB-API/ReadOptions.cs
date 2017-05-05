namespace LevelDB
{
    public class ReadOptions
    {
        private bool _verifyChecksums;
        private bool _fillCache = true;
        private ISnapshot _snapshot;

        public ISnapshot Snapshot()
        {
            return _snapshot;
        }

        public ReadOptions Snapshot(ISnapshot snapshot)
        {
            _snapshot = snapshot;
            return this;
        }

        public bool FillCache()
        {
            return _fillCache;
        }

        public ReadOptions FillCache(bool fillCache)
        {
            _fillCache = fillCache;
            return this;
        }

        public bool VerifyChecksums()
        {
            return _verifyChecksums;
        }

        public ReadOptions VerifyChecksums(bool verifyChecksums)
        {
            _verifyChecksums = verifyChecksums;
            return this;
        }
    }
}