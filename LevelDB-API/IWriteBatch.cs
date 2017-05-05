using System;

namespace LevelDB
{
    public interface IWriteBatch : IDisposable
    {
        IWriteBatch Put(byte[] key, byte[] value);

        IWriteBatch Delete(byte[] key);
    }
}