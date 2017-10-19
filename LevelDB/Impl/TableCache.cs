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
using System.IO;
using LevelDB.Guava;
using LevelDB.Table;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class TableCache
    {
        private readonly LRUCache<long, TableAndFile> _cache;

        //private readonly Finalizer<Table.Table> finalizer = new Finalizer<>(1);
        private readonly Func<long, TableAndFile> _valueFactory;

        public TableCache(DirectoryInfo databaseDir, int tableCacheSize, IUserComparator userComparator,
            bool verifyChecksums)
        {
            Preconditions.CheckNotNull(databaseDir, $"{nameof(databaseDir)} is null");

            _cache = new LRUCache<long, TableAndFile>(tableCacheSize);
            _cache.OnRemove += (sender, args) =>
            {
                var tableAndFile = args.Value;
                if (tableAndFile != null)
                {
                    var table = tableAndFile.Table;
                    table.Dispose();
                    //finalizer.addCleanup(table, table.closer()
                }
            };

            _valueFactory = fileNumber =>
            {
                try
                {
                    return new TableAndFile(databaseDir,
                        fileNumber,
                        userComparator,
                        verifyChecksums);
                }
                catch (Exception e)
                {
                    throw new Exception($"Could not open table: {fileNumber}", e.InnerException);
                }
            };
        }

        public InternalTableIterator NewIterator(FileMetaData file)
        {
            return NewIterator(file.Number);
        }

        public InternalTableIterator NewIterator(long number)
        {
            return new InternalTableIterator(GetTable(number).GetTableIterator());
        }

        public long GetApproximateOffsetOf(FileMetaData file, Slice key)
        {
            return GetTable(file.Number).GetApproximateOffsetOf(key);
        }

        private Table.Table GetTable(long number)
        {
            return _cache.GetOrAdd(number, _valueFactory).Table;
        }

        public void Close()
        {
            _cache.Clear();
            //finalizer.destroy();
        }

        public void Evict(long number)
        {
            _cache.Remove(number);
        }

        internal sealed class TableAndFile
        {
            public Table.Table Table { get; }
            public readonly FileStream FileChannel;

            internal TableAndFile(FileSystemInfo databaseDir, long fileNumber, IUserComparator userComparator,
                bool verifyChecksums)
            {
                var tableFileName = Filename.TableFileName(fileNumber);
                var tableFile = new FileInfo(Path.Combine(databaseDir.FullName, tableFileName));
                FileChannel = tableFile.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                try
                {
                    if (DBFactory.UseMMap)
                    {
                        Table = new MMapTable(tableFile.FullName, FileChannel, userComparator, verifyChecksums);
                    }
                    else
                    {
                        Table = new FileChannelTable(tableFile.FullName, FileChannel, userComparator, verifyChecksums);
                    }
                }
                catch
                    (IOException)
                {
                    Disposables.DisposeQuietly(FileChannel);
                    throw;
                }
            }
        }
    }
}