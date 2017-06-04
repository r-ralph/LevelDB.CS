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
using LevelDB.Impl;

namespace LevelDB.Util
{
    public sealed class MergingIterator : AbstractSeekingIterator<InternalKey, Slice>
    {
        private readonly IList<IInternalIterator> _levels;
        private readonly PriorityQueue<ComparableIterator> _priorityQueue;
        private readonly IComparer<InternalKey> _comparer;

        public MergingIterator(IList<IInternalIterator> levels, IComparer<InternalKey> comparer)
        {
            _levels = levels;
            _comparer = comparer;

            _priorityQueue = new PriorityQueue<ComparableIterator>(levels.Count + 1);
            ResetPriorityQueue(comparer);
        }

        protected override void SeekToFirstInternal()
        {
            foreach (var level in _levels)
            {
                level.SeekToFirst();
            }
            ResetPriorityQueue(_comparer);
        }

        protected override void SeekInternal(InternalKey targetKey)
        {
            foreach (var level in _levels)
            {
                level.Seek(targetKey);
            }
            ResetPriorityQueue(_comparer);
        }

        protected override Entry<InternalKey, Slice> GetNextElement()
        {
            var nextIterator = _priorityQueue.Peek();
            if (nextIterator == null)
            {
                return null;
            }
            var result = nextIterator.Next();
            if (nextIterator.HasNext())
            {
                _priorityQueue.Enqueue(nextIterator);
            }
            return result;
        }

        private void ResetPriorityQueue(IComparer<InternalKey> comparator)
        {
            var i = 1;
            foreach (var level in _levels)
            {
                if (level.HasNext())
                {
                    _priorityQueue.Enqueue(new ComparableIterator(level, comparator, i++, level.Next()));
                }
            }
        }

        public override string ToString()
        {
            return $"MergingIterator(levels={_levels}, comparer={_comparer})";
        }

        private class ComparableIterator : IEnumerator<Entry<InternalKey, Slice>>, IComparable<ComparableIterator>
        {
            private readonly IInternalIterator _iterator;
            private readonly IComparer<InternalKey> _comparator;
            private readonly int _ordinal;
            private Entry<InternalKey, Slice> _nextElement;

            internal ComparableIterator(IInternalIterator iterator, IComparer<InternalKey> comparator, int ordinal,
                Entry<InternalKey, Slice> nextElement)
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
                    throw new ArgumentException();
                }

                var result = _nextElement;
                _nextElement = _iterator.HasNext() ? _iterator.Next() : null;
                return result;
            }

            public override bool Equals(Object o)
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
            }

            public bool MoveNext()
            {
                throw new NotSupportedException();
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public Entry<InternalKey, Slice> Current => null;

            object IEnumerator.Current => Current;
        }
    }
}