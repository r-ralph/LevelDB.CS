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
using System.Collections.Generic;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class WriteBatchImpl : IWriteBatch<WriteBatchImpl>
    {
        private readonly IList<Entry<Slice, Slice>> _batch = new List<Entry<Slice, Slice>>();

        public int ApproximateSize { get; private set; }

        public int Size => _batch.Count;

        public WriteBatchImpl Put(byte[] key, byte[] value)
        {
            Preconditions.CheckNotNull(key, $"{key} is null");
            Preconditions.CheckNotNull(value, $"{value} is null");
            _batch.Add(new ImmutableEntry<Slice, Slice>(Slices.WrappedBuffer(key), Slices.WrappedBuffer(value)));
            ApproximateSize += 12 + key.Length + value.Length;
            return this;
        }

        public WriteBatchImpl Put(Slice key, Slice value)
        {
            Preconditions.CheckNotNull(key, $"{key} is null");
            Preconditions.CheckNotNull(value, $"{value} is null");
            _batch.Add(new ImmutableEntry<Slice, Slice>(key, value));
            ApproximateSize += 12 + key.Length + value.Length;
            return this;
        }

        public WriteBatchImpl Delete(byte[] key)
        {
            Preconditions.CheckNotNull(key, $"{key} is null");
            _batch.Add(new ImmutableEntry<Slice, Slice>(Slices.WrappedBuffer(key), null));
            ApproximateSize += 6 + key.Length;
            return this;
        }

        public WriteBatchImpl Delete(Slice key)
        {
            Preconditions.CheckNotNull(key, $"{key} is null");
            _batch.Add(new ImmutableEntry<Slice, Slice>(key, null));
            ApproximateSize += 6 + key.Length;
            return this;
        }

        public void Dispose()
        {
        }

        public void ForEach(IHandler handler)
        {
            foreach (var entry in _batch)
            {
                var key = entry.Key;
                var value = entry.Value;
                if (value != null)
                {
                    handler.Put(key, value);
                }
                else
                {
                    handler.Delete(key);
                }
            }
        }

        public void ForEach(Action<Slice, Slice> putAction, Action<Slice> deleteAction)
        {
            foreach (var entry in _batch)
            {
                var key = entry.Key;
                var value = entry.Value;
                if (value != null)
                {
                    putAction(key, value);
                }
                else
                {
                    deleteAction(key);
                }
            }
        }

        public interface IHandler
        {
            void Put(Slice key, Slice value);

            void Delete(Slice key);
        }
    }
}