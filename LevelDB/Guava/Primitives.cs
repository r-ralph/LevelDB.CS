namespace LevelDB.Guava
{
    public static class Primitives
    {
        public static int Compare(long a, long b)
        {
            return a < b ? -1 : (a > b ? 1 : 0);
        }
    }
}