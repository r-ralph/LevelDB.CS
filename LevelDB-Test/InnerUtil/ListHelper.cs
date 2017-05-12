using System.Collections.Generic;

namespace LevelDB.InnerUtil
{
    public static class ListHelper
    {
        public static List<T> Sublist<T>(List<T> list, int fromIndex, int toIndex)
        {
            var count = toIndex - fromIndex;
            return list.GetRange(fromIndex, count);
        }
    }
}