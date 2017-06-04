#region Copyright

// Copyright 2017 Ralph (Tamaki Hidetsugu)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

using System;
using System.Collections;
using System.Collections.Generic;
using LevelDB.Guava;
using LevelDB.Impl;

namespace LevelDB.Util
{
    public class DbIterator : AbstractSeekingIterator<InternalKey, Slice>, IInternalIterator
    {
        private readonly MemTable.MemTableIterator _memTableIterator;
        private readonly MemTable.MemTableIterator _immutableMemTableIterator;
        private readonly IList<InternalTableIterator> _level0Files;
        private readonly IList<LevelIterator> _levels;

        private readonly IComparer<InternalKey> _comparer;

        private readonly ComparableIterator[] _heap;
        private int _heapSize;

        public DbIterator(MemTable.MemTableIterator memTableIterator,
            MemTable.MemTableIterator immutableMemTableIterator,
            IList<InternalTableIterator> level0Files,
            IList<LevelIterator> levels,
            IComparer<InternalKey> comparer)
        {
            _memTableIterator = memTableIterator;
            _immutableMemTableIterator = immutableMemTableIterator;
            _level0Files = level0Files;
            _levels = levels;
            _comparer = comparer;

            _heap = new ComparableIterator[3 + level0Files.Count + levels.Count];
            ResetPriorityQueue();
        }

        protected override void SeekToFirstInternal()
        {
            _memTableIterator?.SeekToFirst();
            _immutableMemTableIterator?.SeekToFirst();
            foreach (var level0File in _level0Files)
            {
                level0File.SeekToFirst();
            }
            foreach (var level in _levels)
            {
                level.SeekToFirst();
            }
            ResetPriorityQueue();
        }

        protected override void SeekInternal(InternalKey targetKey)
        {
            _memTableIterator?.Seek(targetKey);
            _immutableMemTableIterator?.Seek(targetKey);
            foreach (var level0File in _level0Files)
            {
                level0File.Seek(targetKey);
            }
            foreach (var level in _levels)
            {
                level.Seek(targetKey);
            }
            ResetPriorityQueue();
        }

        protected override Entry<InternalKey, Slice> GetNextElement()
        {
            if (_heapSize == 0)
            {
                return null;
            }

            var smallest = _heap[0];
            var result = smallest.Next();

            // if the smallest iterator has more elements, put it back in the heap,
            // otherwise use the last element in the queue
            ComparableIterator replacementElement;
            if (smallest.HasNext())
            {
                replacementElement = smallest;
            }
            else
            {
                _heapSize--;
                replacementElement = _heap[_heapSize];
                _heap[_heapSize] = null;
            }

            if (replacementElement == null) return result;
            _heap[0] = replacementElement;
            HeapSiftDown(0);

            return result;
        }

        private void ResetPriorityQueue()
        {
            var i = 0;
            _heapSize = 0;
            if (_memTableIterator != null && _memTableIterator.HasNext())
            {
                HeapAdd(new ComparableIterator(_memTableIterator, _comparer, i++, _memTableIterator.Next()));
            }
            if (_immutableMemTableIterator != null && _immutableMemTableIterator.HasNext())
            {
                HeapAdd(new ComparableIterator(_immutableMemTableIterator, _comparer, i++,
                    _immutableMemTableIterator.Next()));
            }
            foreach (var level0File in _level0Files)
            {
                if (level0File.HasNext())
                {
                    HeapAdd(new ComparableIterator(level0File, _comparer, i++, level0File.Next()));
                }
            }
            foreach (var level in _levels)
            {
                if (level.HasNext())
                {
                    HeapAdd(new ComparableIterator(level, _comparer, i++, level.Next()));
                }
            }
        }

        private void HeapAdd(ComparableIterator newElement)
        {
            Preconditions.CheckNotNull(newElement, $"{nameof(newElement)} is null");
            _heap[_heapSize] = newElement;
            HeapSiftUp(_heapSize++);
        }

        private void HeapSiftUp(int childIndex)
        {
            var target = _heap[childIndex];
            while (childIndex > 0)
            {
                var parentIndex = (childIndex - 1) / 2;
                var parent = _heap[parentIndex];
                if (parent.CompareTo(target) <= 0)
                {
                    break;
                }
                _heap[childIndex] = parent;
                childIndex = parentIndex;
            }
            _heap[childIndex] = target;
        }

        private void HeapSiftDown(int rootIndex)
        {
            var target = _heap[rootIndex];
            int childIndex;
            while ((childIndex = rootIndex * 2 + 1) < _heapSize)
            {
                if (childIndex + 1 < _heapSize
                    && _heap[childIndex + 1].CompareTo(_heap[childIndex]) < 0)
                {
                    childIndex++;
                }
                if (target.CompareTo(_heap[childIndex]) <= 0)
                {
                    break;
                }
                _heap[rootIndex] = _heap[childIndex];
                rootIndex = childIndex;
            }
            _heap[rootIndex] = target;
        }

        public override string ToString()
        {
            return
                $"DbIterator(memTableIterator={_memTableIterator}, immutableMemTableIterator={_immutableMemTableIterator}, level0Files={_level0Files}, levels={_levels}, comparer={_comparer})";
        }

        public class ComparableIterator : IEnumerator<Entry<InternalKey, Slice>>, IComparable<ComparableIterator>
        {
            private readonly ISeekingIterator<InternalKey, Slice> _iterator;
            private readonly IComparer<InternalKey> _comparator;
            private readonly int _ordinal;
            private Entry<InternalKey, Slice> _nextElement;

            internal ComparableIterator(ISeekingIterator<InternalKey, Slice> iterator,
                IComparer<InternalKey> comparator,
                int ordinal, Entry<InternalKey, Slice> nextElement)
            {
                _iterator = iterator;
                _comparator = comparator;
                _ordinal = ordinal;
                _nextElement = nextElement;
            }

            public bool HasNext()
            {
                return _nextElement != null;
            }

            public Entry<InternalKey, Slice> Next()
            {
                if (_nextElement == null)
                {
                    throw new InvalidOperationException();
                }

                var result = _nextElement;
                _nextElement = _iterator.HasNext() ? _iterator.Next() : null;
                return result;
            }


            public override bool Equals(object o)
            {
                if (this == o)
                {
                    return true;
                }

                var comparableIterator = o as ComparableIterator;

                if (_ordinal != comparableIterator?._ordinal)
                {
                    return false;
                }
                return !(!_nextElement?.Equals(comparableIterator._nextElement) ??
                         comparableIterator._nextElement != null);
            }

            public override int GetHashCode()
            {
                var result = _ordinal;
                result = 31 * result + (_nextElement != null ? _nextElement.GetHashCode() : 0);
                return result;
            }

            public int CompareTo(ComparableIterator that)
            {
                var result = _comparator.Compare(_nextElement.Key, that._nextElement.Key);
                if (result == 0)
                {
                    result = _ordinal.CompareTo(that._ordinal);
                }
                return result;
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }

            public bool MoveNext()
            {
                throw new NotImplementedException();
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public Entry<InternalKey, Slice> Current { get; }

            object IEnumerator.Current => Current;
        }
    }
}