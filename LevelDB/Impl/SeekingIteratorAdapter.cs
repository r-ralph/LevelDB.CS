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
using LevelDB.Util;
using LevelDB.Util.Atomic;

namespace LevelDB.Impl
{
    public class SeekingIteratorAdapter : IDBIterator<SeekingIteratorAdapter.DbEntry>
    {
        private readonly SnapshotSeekingIterator _seekingIterator;
        private readonly AtomicBoolean _closed = new AtomicBoolean(false);

        public SeekingIteratorAdapter(SnapshotSeekingIterator seekingIterator)
        {
            _seekingIterator = seekingIterator;
        }

        public void SeekToFirst()
        {
            _seekingIterator.SeekToFirst();
        }

        public void Seek(byte[] key)
        {
            _seekingIterator.Seek(Slices.WrappedBuffer(key));
        }

        public bool HasNext()
        {
            return _seekingIterator.HasNext();
        }

        public DbEntry Next()
        {
            return Adapt(_seekingIterator.Next());
        }

        public DbEntry PeekNext()
        {
            return Adapt(_seekingIterator.Peek());
        }

        public void Dispose()
        {
            // This is an end user API.. he might screw up and close multiple times.
            // but we don't want the close multiple times as reference counts go bad.
            if (_closed.CompareAndSet(false, true))
            {
                _seekingIterator.Dispose();
            }
        }

        public IEnumerator<DbEntry> GetEnumerator()
        {
            throw new NotSupportedException();
        }

        private static DbEntry Adapt(Entry<Slice, Slice> entry)
        {
            return new DbEntry(entry.Key, entry.Value);
        }

        #region Not supported methods

        void IDBIterator<DbEntry>.SeekToLast()
        {
            throw new NotSupportedException();
        }

        public bool HasPrev()
        {
            throw new NotSupportedException();
        }

        public DbEntry Prev()
        {
            throw new NotSupportedException();
        }

        public DbEntry PeekPrev()
        {
            throw new NotSupportedException();
        }

        #endregion

        public class DbEntry : Entry<byte[], byte[]>
        {
            public override byte[] Key => KeySlice.GetBytes();

            public override byte[] Value => ValueSlice.GetBytes();

            public Slice KeySlice { get; }
            public Slice ValueSlice { get; }

            public DbEntry(Slice key, Slice value)
            {
                Preconditions.CheckNotNull(key, "key is null");
                Preconditions.CheckNotNull(value, "value is null");
                KeySlice = key;
                ValueSlice = value;
            }

            public override bool Equals(object obj)
            {
                var that = obj as Entry<byte[], byte[]>;
                if (that != null)
                {
                    return Key.Equals(that.Key) && Value.Equals(that.Value);
                }
                return false;
            }

            public override int GetHashCode()
            {
                return Key.GetHashCode() ^ Value.GetHashCode();
            }

            /// <summary>
            /// Returns a string representation of the form <code>{key}={value}</code>.
            /// </summary>
            /// <returns></returns>
            public override string ToString()
            {
                return Key + "=" + Value;
            }
        }

        public bool MoveNext()
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public DbEntry Current { get; }

        object IEnumerator.Current
        {
            get { return Current; }
        }
    }
}