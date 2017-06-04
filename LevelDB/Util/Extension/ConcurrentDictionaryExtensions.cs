using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace LevelDB.Util.Extension
{
    public static class ConcurrentDictionaryExtensions
    {
        private static Tuple<int, int> GetPossibleIndices<TKey, TValue>(ConcurrentDictionary<TKey, TValue> dictionary,
            TKey key, IComparer<TKey> comparer, bool strictlyDifferent, out List<TKey> list)
        {
            list = dictionary.Keys.ToList();
            var index = list.BinarySearch(key, comparer);
            if (index >= 0)
            {
                // exists
                return strictlyDifferent ? Tuple.Create(index - 1, index + 1) : Tuple.Create(index, index);
            }
            // doesn't exist
            var indexOfBiggerNeighbour = ~index; //bitwise complement of the return value

            if (indexOfBiggerNeighbour == list.Count)
            {
                // bigger than all elements
                return Tuple.Create(list.Count - 1, list.Count);
            }
            if (indexOfBiggerNeighbour == 0)
            {
                // smaller than all elements
                return Tuple.Create(-1, 0);
            }
            // Between 2 elements
            var indexOfSmallerNeighbour = indexOfBiggerNeighbour - 1;
            return Tuple.Create(indexOfSmallerNeighbour, indexOfBiggerNeighbour);
        }

        public static TKey LowerKey<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dictionary, TKey key,
            IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, true, out list);
            return indices.Item1 < 0 ? default(TKey) : list[indices.Item1];
        }

        public static KeyValuePair<TKey, TValue>? LowerEntry<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary, TKey key, IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, true, out list);
            if (indices.Item1 < 0)
                return null;

            var newKey = list[indices.Item1];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey FloorKey<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dictionary, TKey key,
            IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, false, out list);
            return indices.Item1 < 0 ? default(TKey) : list[indices.Item1];
        }

        public static KeyValuePair<TKey, TValue>? FloorEntry<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary, TKey key, IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, false, out list);
            if (indices.Item1 < 0)
                return null;

            var newKey = list[indices.Item1];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey CeilingKey<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dictionary, TKey key,
            IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, false, out list);
            return indices.Item2 == list.Count ? default(TKey) : list[indices.Item2];
        }

        public static KeyValuePair<TKey, TValue>? CeilingEntry<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary, TKey key, IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, false, out list);
            if (indices.Item2 == list.Count)
                return null;

            var newKey = list[indices.Item2];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }

        public static TKey HigherKey<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> dictionary, TKey key,
            IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, true, out list);
            return indices.Item2 == list.Count ? default(TKey) : list[indices.Item2];
        }

        public static KeyValuePair<TKey, TValue>? HigherEntry<TKey, TValue>(
            this ConcurrentDictionary<TKey, TValue> dictionary, TKey key, IComparer<TKey> comparer)
        {
            List<TKey> list;
            var indices = GetPossibleIndices(dictionary, key, comparer, true, out list);
            if (indices.Item2 == list.Count)
                return null;

            var newKey = list[indices.Item2];
            return new KeyValuePair<TKey, TValue>(newKey, dictionary[newKey]);
        }
    }
}