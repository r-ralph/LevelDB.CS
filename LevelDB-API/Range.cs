namespace LevelDB
{
    public class Range
    {
        private readonly byte[] _start;
        private readonly byte[] _limit;

        public byte[] Limit()
        {
            return _limit;
        }

        public byte[] Start()
        {
            return _start;
        }

        public Range(byte[] start, byte[] limit)
        {
            Options.CheckArgNotNull(start, "start");
            Options.CheckArgNotNull(limit, "limit");
            _limit = limit;
            _start = start;
        }
    }
}