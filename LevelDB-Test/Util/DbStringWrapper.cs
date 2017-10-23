using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Text;
using LevelDB.Impl;
using ValueType = LevelDB.Impl.ValueType;

namespace LevelDB.Util
{
    public class DbStringWrapper : IDisposable
    {
        private readonly Options options;
        private readonly DirectoryInfo databaseDir;
        private DbImpl db;

        public DbStringWrapper(Options options, DirectoryInfo databaseDir)
        {
            this.options = options.VerifyChecksums(true).CreateIfMissing(true).ErrorIfExists(false);
            this.databaseDir = databaseDir;
            this.db = new DbImpl(options, databaseDir);
            //opened.add(this);
        }

        public string Get(string key)
        {
            byte[] slice = db.Get(Utils.ToByteArray(key));
            if (slice == null)
            {
                return null;
            }
            return Utils.ToString(slice);
        }

        public string Get(string key, ISnapshot snapshot)
        {
            byte[] slice = db.Get(Utils.ToByteArray(key), new ReadOptions().Snapshot(snapshot));
            if (slice == null)
            {
                return null;
            }
            return Utils.ToString(slice);
        }

        public void Put(string key, string value)
        {
            db.Put(Utils.ToByteArray(key), Utils.ToByteArray(value));
        }

        public void Delete(string key)
        {
            db.Delete(Utils.ToByteArray(key));
        }

        public ISeekingIterator<string, string> iterator()
        {
            return new StringDbIterator(db.GetEnumerator());
        }

        public ISnapshot GetSnapshot()
        {
            return db.GetSnapshot();
        }

        public void Dispose()
        {
            db.Dispose();
        }

        public void CompactMemTable()
        {
            db.FlushMemTable();
        }

        public void CompactRange(int level, string start, string limit)
        {
            db.CompactRange(level, Slices.CopiedBuffer(start, Encoding.UTF8),
                Slices.CopiedBuffer(limit, Encoding.UTF8));
        }

        public void Compact(string start, string limit)
        {
            db.FlushMemTable();
            int maxLevelWithFiles = 1;
            for (int level = 2; level < DbConstants.NumLevels; level++)
            {
                if (db.NumberOfFilesInLevel(level) > 0)
                {
                    maxLevelWithFiles = level;
                }
            }
            for (int level = 0; level < maxLevelWithFiles; level++)
            {
                db.CompactRange(level, Slices.CopiedBuffer("", Encoding.UTF8), Slices.CopiedBuffer("~", Encoding.UTF8));
            }
        }

        public int NumberOfFilesInLevel(int level)
        {
            return db.NumberOfFilesInLevel(level);
        }

        public int TotalTableFiles()
        {
            int result = 0;
            for (int level = 0; level < DbConstants.NumLevels; level++)
            {
                result += db.NumberOfFilesInLevel(level);
            }
            return result;
        }

        public long Size(string start, string limit)
        {
            return db.GetApproximateSizes(new Range(Utils.ToByteArray(start), Utils.ToByteArray(limit)));
        }

        public long GetMaxNextLevelOverlappingBytes()
        {
            return db.GetMaxNextLevelOverlappingBytes();
        }

        public void Reopen()
        {
            Reopen(options);
        }

        public void Reopen(Options options)
        {
            db.Dispose();
            db = new DbImpl(options.VerifyChecksums(true).CreateIfMissing(false).ErrorIfExists(false), databaseDir);
        }

        public IList<string> AllEntriesFor(String userKey)
        {
            ImmutableList<string>.Builder result = ImmutableList.CreateBuilder<string>();
            foreach (var entry in db.InternalIterable())
            {
                string entryKey = entry.Key.UserKey.ToString(Encoding.UTF8);
                if (entryKey.Equals(userKey))
                {
                    if (entry.Key.ValueType == ValueType.Value)
                    {
                        result.Add(entry.Value.ToString(Encoding.UTF8));
                    }
                    else
                    {
                        result.Add("DEL");
                    }
                }
            }
            return result.ToImmutable();
        }

        private class StringDbIterator : ISeekingIterator<String, String>
        {
            private readonly IDBIterator<Entry<byte[], byte[]>> iterator;

            public StringDbIterator(IDBIterator<Entry<byte[], byte[]>> iterator)
            {
                this.iterator = iterator;
            }

            public bool HasNext()
            {
                return iterator.HasNext();
            }

            public void SeekToFirst()
            {
                iterator.SeekToFirst();
            }

            public void Seek(string targetKey)
            {
                iterator.Seek(Utils.ToByteArray(targetKey));
            }

            public Entry<string, string> Peek()
            {
                return Adapt(iterator.PeekNext());
            }


            public Entry<string, string> Next()
            {
                return Adapt(iterator.Next());
            }


            public Entry<string, string> Remove()
            {
                throw new NotSupportedException();
            }

            private Entry<String, String> Adapt(Entry<byte[], byte[]> next)
            {
                return new ImmutableEntry<string, string>(Utils.ToString(next.Key), Utils.ToString(next.Value));
            }

            public bool MoveNext()
            {
                throw new NotImplementedException();
            }

            public void Reset()
            {
                throw new NotImplementedException();
            }

            public Entry<string, string> Current { get; }

            object IEnumerator.Current
            {
                get { return Current; }
            }

            public void Dispose()
            {
                throw new NotImplementedException();
            }
        }
    }
}