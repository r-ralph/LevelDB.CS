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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using LevelDB.Guava;
using LevelDB.Util;
using LevelDB.Util.Extension;

namespace LevelDB.Impl
{
    public class MemTable : ISeekingIterable<InternalKey, Slice>
    {
        public bool IsEmpty => _table.IsEmpty;

        public long ApproximateMemoryUsage
        {
            get => Interlocked.Read(ref _approximateMemoryUsage);
            set => Interlocked.Exchange(ref _approximateMemoryUsage, value);
        }

        private readonly ConcurrentDictionary<InternalKey, Slice> _table;
        private readonly InternalKeyComparator _internalKeyComparator;

        private long _approximateMemoryUsage;

        public MemTable(InternalKeyComparator internalKeyComparator)
        {
            _internalKeyComparator = internalKeyComparator;
            _table = new ConcurrentDictionary<InternalKey, Slice>(internalKeyComparator);
        }

        public void Add(long sequenceNumber, ValueType valueType, Slice key, Slice value)
        {
            Preconditions.CheckNotNull(valueType, $"{nameof(valueType)} is null");
            Preconditions.CheckNotNull(key, $"{nameof(key)} is null");

            var internalKey = new InternalKey(key, sequenceNumber, valueType);
            _table[internalKey] = value;

            ApproximateMemoryUsage = key.Length + SizeOf.Long + value.Length;
        }

        public LookupResult Get(LookupKey key)
        {
            Preconditions.CheckNotNull(key, "key is null");

            var internalKey = key.InternalKey;
            var entry = _table.CeilingEntry(internalKey, _internalKeyComparator);
            if (!entry.HasValue)
            {
                return null;
            }

            var entryKey = entry.Value.Key;
            if (entryKey.UserKey.Equals(key.UserKey))
            {
                return entryKey.ValueType == ValueType.Deletion
                    ? LookupResult.Deleted(key)
                    : LookupResult.Ok(key, entry.Value.Value);
            }
            return null;
        }

        public MemTableIterator GetMemTableIterator()
        {
            return new MemTableIterator(_table, _internalKeyComparator);
        }

        public IEnumerator<Entry<InternalKey, Slice>> GetEnumerator()
        {
            return GetMemTableIterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetMemTableIterator();
        }

        public class MemTableIterator : IInternalIterator
        {
            private IPeekingIterator<Entry<InternalKey, Slice>> iterator;
            private ConcurrentDictionary<InternalKey, Slice> _internalTable;
            private readonly InternalKeyComparator _internalKeyComparator;

            public MemTableIterator(ConcurrentDictionary<InternalKey, Slice> table,
                InternalKeyComparator internalKeyComparator)
            {
                _internalTable = table;
                _internalKeyComparator = internalKeyComparator;
                iterator = Iterators.PeekingIterator(_internalTable.GetEnumerator());
            }

            public bool HasNext()
            {
                return iterator.HasNext();
            }

            public void SeekToFirst()
            {
                iterator = Iterators.PeekingIterator(_internalTable.GetEnumerator());
            }

            public void Seek(InternalKey targetKey)
            {
                iterator = Iterators.PeekingIterator(_internalTable
                    .Where(pair => _internalKeyComparator.Compare(targetKey, pair.Key) >= 0)
                    .GetEnumerator());
            }

            public Entry<InternalKey, Slice> Peek()
            {
                var entry = iterator.Peek();
                return new InternalEntry(entry.Key, entry.Value);
            }

            public Entry<InternalKey, Slice> Next()
            {
                var entry = iterator.Next();
                return new InternalEntry(entry.Key, entry.Value);
            }

            #region Not supported

            public Entry<InternalKey, Slice> Remove()
            {
                throw new NotSupportedException();
            }

            public bool MoveNext()
            {
                if (!HasNext()) return false;
                _current = Next();
                return true;
            }

            public void Reset()
            {
                SeekToFirst();
            }

            private Entry<InternalKey, Slice> _current;

            public Entry<InternalKey, Slice> Current
            {
                get
                {
                    if (_current == null && HasNext())
                    {
                        _current = Next();
                    }
                    return _current;
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                Reset();
            }

            #endregion
        }
    }
}