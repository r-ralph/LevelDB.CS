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
using System.Diagnostics.CodeAnalysis;

namespace LevelDB
{
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    public interface DB : IEnumerable<KeyValuePair<byte[], byte[]>>, IDisposable
    {
        byte[] Get(byte[] key);

        byte[] Get(byte[] key, ReadOptions options);

        new IDBIterator GetEnumerator();

        IDBIterator GetEnumerator(ReadOptions options);

        void Put(byte[] key, byte[] value);

        void Delete(byte[] key);

        void Write(IWriteBatch updates);

        IWriteBatch CreateWriteBatch();

        ISnapshot Put(byte[] key, byte[] value, WriteOptions options);

        ISnapshot Delete(byte[] key, WriteOptions options);

        ISnapshot Write(IWriteBatch updates, WriteOptions options);

        ISnapshot GetSnapshot();

        long[] GetApproximateSizes(params Range[] ranges);

        string GetProperty(string name);

        void SuspendCompactions();

        void ResumeCompactions();

        void CompactRange(byte[] begin, byte[] end);
    }
}