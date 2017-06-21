using System.IO;

namespace LevelDB.Util
{
    public static class FileUtil
    {
        public static void DeleteRecursively(DirectoryInfo directory)
        {
            if (!directory.Exists)
            {
                return;
            }
            foreach (var file in directory.GetFiles())
            {
                file.Delete();
            }
            foreach (var dir in directory.GetDirectories())
            {
                dir.Delete(true);
            }
        }
    }
}