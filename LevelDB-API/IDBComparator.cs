using System.Collections.Generic;

namespace LevelDB
{
    public interface IDBComparator : IComparer<byte[]>
    {
        string Name();

        byte[] FindShortestSeparator(byte[] start, byte[] limit);

        byte[] FindShortSuccessor(byte[] key);
    }
}