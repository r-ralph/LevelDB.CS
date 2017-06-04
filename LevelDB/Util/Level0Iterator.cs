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
using System.Collections.Immutable;
using LevelDB.Impl;

namespace LevelDB.Util
{
    public class Level0Iterator : AbstractSeekingIterator<InternalKey, Slice>, IInternalIterator
    {
        private readonly IList<InternalTableIterator> _inputs;
        private readonly PriorityQueue<ComparableIterator> _priorityQueue;
        private readonly IComparer<InternalKey> _comparator;

        public Level0Iterator(TableCache tableCache, IEnumerable<FileMetaData> files, IComparer<InternalKey> comparator)
        {
            var builder = ImmutableList.CreateBuilder<InternalTableIterator>();
            foreach (var file in files)
            {
                builder.Add(tableCache.NewIterator(file));
            }
            _inputs = builder.ToImmutable();
            _comparator = comparator;

            _priorityQueue = new PriorityQueue<ComparableIterator>(_inputs.Count + 1);
            ResetPriorityQueue(comparator);
        }

        public Level0Iterator(List<InternalTableIterator> inputs, IComparer<InternalKey> comparator)
        {
            _inputs = inputs;
            _comparator = comparator;

            _priorityQueue = new PriorityQueue<ComparableIterator>(inputs.Count + 1);
            ResetPriorityQueue(comparator);
        }

        protected override void SeekToFirstInternal()
        {
            foreach (var input in _inputs)
            {
                input.SeekToFirst();
            }
            ResetPriorityQueue(_comparator);
        }

        protected override void SeekInternal(InternalKey targetKey)
        {
            foreach (var input in _inputs)
            {
                input.Seek(targetKey);
            }
            ResetPriorityQueue(_comparator);
        }

        private void ResetPriorityQueue(IComparer<InternalKey> comparator)
        {
            var i = 0;
            foreach (var input in _inputs)
            {
                if (input.HasNext())
                {
                    _priorityQueue.Enqueue(new ComparableIterator(input, comparator, i++, input.Next()));
                }
            }
        }

        protected override Entry<InternalKey, Slice> GetNextElement()
        {
            var nextIterator = _priorityQueue.Dequeue();
            if (nextIterator == null) return null;
            var result = nextIterator.Next();
            if (nextIterator.HasNext())
            {
                _priorityQueue.Enqueue(nextIterator);
            }
            return result;
        }

        public override string ToString()
        {
            return $"Level0Iterator(inputs={_inputs}, comparator={_comparator})";
        }

        private class ComparableIterator : IEnumerator<Entry<InternalKey, Slice>>, IComparable<ComparableIterator>
        {
            private readonly ISeekingIterator<InternalKey, Slice> _iterator;
            private readonly IComparer<InternalKey> _comparator;

            private readonly int _ordinal;
            private Entry<InternalKey, Slice> _nextElement;

            public ComparableIterator(ISeekingIterator<InternalKey, Slice> iterator, IComparer<InternalKey> comparator,
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

            public int CompareTo(ComparableIterator other)
            {
                var result = _comparator.Compare(_nextElement.Key, other._nextElement.Key);
                if (result == 0)
                {
                    result = _ordinal.CompareTo(other._ordinal);
                }
                return result;
            }

            #region Not supported

            public void Dispose()
            {
                throw new NotSupportedException();
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

            #endregion
        }
    }
}