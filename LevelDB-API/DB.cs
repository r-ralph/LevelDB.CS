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