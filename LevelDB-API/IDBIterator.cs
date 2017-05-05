using System;
using System.Collections.Generic;

namespace LevelDB
{
    public interface IDBIterator : IEnumerable<KeyValuePair<byte[], byte[]>>, IDisposable
    {
        void Seek(byte[] key);

        void SeekToFirst();

        KeyValuePair<byte[], byte[]> PeekNext();

        bool HasPrev();

        KeyValuePair<byte[], byte[]> Prev();

        KeyValuePair<byte[], byte[]> PeekPrev();

        void SeekToLast();
    }
}