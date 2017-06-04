using System;
using System.IO;

namespace LevelDB.Util.Extension
{
    public static class FileInfoExtensions
    {
        public static bool Rename(this FileInfo fileInfo, FileInfo newFileInfo)
        {
            try
            {
                if (newFileInfo.Exists)
                {
                    newFileInfo.Delete();
                }
                fileInfo.MoveTo(newFileInfo.FullName);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public static bool IsReadable(this FileInfo fileInfo)
        {
            return (fileInfo.Attributes & FileAttributes.ReadOnly) != 0;
        }
    }
}