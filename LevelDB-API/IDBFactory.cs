using System.IO;

namespace LevelDB
{
    public interface IDBFactory
    {
        DB Open(DirectoryInfo path, Options options);

        void Destroy(DirectoryInfo path, Options options);

        void Repair(DirectoryInfo path, Options options);
    }
}