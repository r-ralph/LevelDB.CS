using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Text;
using LevelDB.Guava;
using LevelDB.Util;
using LevelDB.Util.Extension;

namespace LevelDB.Impl
{
    public static class Filename
    {
        public enum FileType
        {
            Log,
            DBLock,
            Table,
            Descriptor,
            Current,
            Temp,
            InfoLog // Either the current one, or an old one
        }

        /// <summary>
        /// Return the name of the log file with the specified number.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public static string LogFileName(long number)
        {
            return MakeFileName(number, "log");
        }

        /// <summary>
        /// Return the name of the sstable with the specified number.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public static string TableFileName(long number)
        {
            return MakeFileName(number, "ldb");
        }

        /// <summary>
        /// Return the name of the descriptor file with the specified incarnation number.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public static string DescriptorFileName(long number)
        {
            Preconditions.CheckArgument(number >= 0, $"{number} is negative");
            return string.Format("MANIFEST-%06d", number);
        }

        /// <summary>
        /// Return the name of the current file.
        /// </summary>
        /// <returns></returns>
        public static string CurrentFileName()
        {
            return "CURRENT";
        }

        /// <summary>
        /// Return the name of the lock file.
        /// </summary>
        /// <returns></returns>
        public static string LockFileName()
        {
            return "LOCK";
        }

        /// <summary>
        /// Return the name of a temporary file with the specified number.
        /// </summary>
        /// <param name="number"></param>
        /// <returns></returns>
        public static string TempFileName(long number)
        {
            return MakeFileName(number, "dbtmp");
        }

        /// <summary>
        /// Return the name of the info log file.
        /// </summary>
        /// <returns></returns>
        public static string InfoLogFileName()
        {
            return "LOG";
        }

        /// <summary>
        /// Return the name of the old info log file.
        /// </summary>
        /// <returns></returns>
        public static string OldInfoLogFileName()
        {
            return "LOG.old";
        }

        /// <summary>
        /// If filename is a leveldb file, store the type of the file in *type.
        /// The number encoded in the filename is stored in *number.  If the
        /// filename was successfully parsed, returns true.  Else return false.
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static LevelDBFileInfo ParseFileName(FileInfo file)
        {
            // Owned filenames have the form:
            //    dbname/CURRENT
            //    dbname/LOCK
            //    dbname/LOG
            //    dbname/LOG.old
            //    dbname/MANIFEST-[0-9]+
            //    dbname/[0-9]+.(log|sst|dbtmp)
            var fileName = file.Name;
            if ("CURRENT".Equals(fileName))
            {
                return new LevelDBFileInfo(FileType.Current);
            }
            else if ("LOCK".Equals(fileName))
            {
                return new LevelDBFileInfo(FileType.DBLock);
            }
            else if ("LOG".Equals(fileName))
            {
                return new LevelDBFileInfo(FileType.InfoLog);
            }
            else if ("LOG.old".Equals(fileName))
            {
                return new LevelDBFileInfo(FileType.InfoLog);
            }
            else if (fileName.StartsWith("MANIFEST-"))
            {
                var fileNumber = long.Parse(RemovePrefix(fileName, "MANIFEST-"));
                return new LevelDBFileInfo(FileType.Descriptor, fileNumber);
            }
            else if (fileName.EndsWith(".log"))
            {
                var fileNumber = long.Parse(RemoveSuffix(fileName, ".log"));
                return new LevelDBFileInfo(FileType.Log, fileNumber);
            }
            else if (fileName.EndsWith(".sst"))
            {
                var fileNumber = long.Parse(RemoveSuffix(fileName, ".sst"));
                return new LevelDBFileInfo(FileType.Table, fileNumber);
            }
            else if (fileName.EndsWith(".dbtmp"))
            {
                var fileNumber = long.Parse(RemoveSuffix(fileName, ".dbtmp"));
                return new LevelDBFileInfo(FileType.Temp, fileNumber);
            }
            return null;
        }

        /// <summary>
        /// Make the CURRENT file point to the descriptor file with the specified number.
        /// </summary>
        /// <param name="databaseDir"></param>
        /// <param name="descriptorNumber"></param>
        /// <returns>true if successful; false otherwise</returns>
        public static bool SetCurrentFile(DirectoryInfo databaseDir, long descriptorNumber)
        {
            var manifest = DescriptorFileName(descriptorNumber);
            var temp = TempFileName(descriptorNumber);

            var tempFile = new FileInfo(Path.Combine(databaseDir.FullName, temp));
            Files.Write(manifest + "\n", tempFile, Encoding.UTF8);
            var to = new FileInfo(Path.Combine(databaseDir.FullName, CurrentFileName()));
            var ok = tempFile.Rename(to);
            if (ok)
            {
                return true;
            }
            tempFile.Delete();
            Files.Write(manifest + "\n", to, Encoding.UTF8);
            return false;
        }

        public static IList<FileInfo> ListFiles(DirectoryInfo dir)
        {
            var files = dir.GetFiles();
            return files == null ? ImmutableList<FileInfo>.Empty : ImmutableList.Create(files);
        }

        private static string MakeFileName(long number, string suffix)
        {
            Preconditions.CheckArgument(number >= 0, $"{nameof(number)} is negative");
            Preconditions.CheckNotNull(suffix, $"{suffix} is null");
            return string.Format("%06d.%s", number, suffix);
        }

        private static string RemovePrefix(string value, string prefix)
        {
            return value.Substring(prefix.Length);
        }

        private static string RemoveSuffix(string value, string suffix)
        {
            return value.Substring(0, value.Length - suffix.Length);
        }

        public class LevelDBFileInfo
        {
            public FileType FileType { get; }
            public long FileNumber { get; }

            public LevelDBFileInfo(FileType fileType) : this(fileType, 0)
            {
            }

            public LevelDBFileInfo(FileType fileType, long fileNumber)
            {
                Preconditions.CheckNotNull(fileType, $"{nameof(fileType)} is null");
                FileType = fileType;
                FileNumber = fileNumber;
            }

            public override bool Equals(object o)
            {
                if (this == o)
                {
                    return true;
                }

                var fileInfo = o as LevelDBFileInfo;

                if (FileNumber != fileInfo?.FileNumber)
                {
                    return false;
                }
                return FileType == fileInfo.FileType;
            }

            public override int GetHashCode()
            {
                var result = FileType.GetHashCode();
                result = 31 * result + (int) (FileNumber ^ (long) ((ulong) FileNumber >> 32));
                return result;
            }

            public override string ToString()
            {
                return $"FileInfo(fileType={FileType}, fileNumber={FileNumber})";
            }
        }
    }
}