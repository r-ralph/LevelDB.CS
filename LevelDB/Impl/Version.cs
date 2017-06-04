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

using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Threading;
using LevelDB.Guava;
using LevelDB.Util;
using static LevelDB.Impl.DbConstants;

namespace LevelDB.Impl
{
    // todo this class should be immutable
    public class Version : ISeekingIterable<InternalKey, Slice>
    {
        private TableCache TableCache => _versionSet.TableCache;
        public InternalKeyComparator InternalKeyComparator => _versionSet.InternalKeyComparator;

        public int CompactionLevel
        {
            get
            {
                lock (_syncCompactionLevel)
                {
                    return _compactionLevel;
                }
            }
            set
            {
                lock (_syncCompactionLevel)
                {
                    _compactionLevel = value;
                }
            }
        }

        public double CompactionScore
        {
            get
            {
                lock (_syncCompactionScore)
                {
                    return _compactionScore;
                }
            }
            set
            {
                lock (_syncCompactionScore)
                {
                    _compactionScore = value;
                }
            }
        }

        private readonly VersionSet _versionSet;
        private readonly Level0 _level0;
        private readonly IList<Level> _levels;

        private readonly object _syncCompactionLevel = new object();
        private readonly object _syncCompactionScore = new object();
        private int _retained = 1;

        // move these mutable fields somewhere else
        private int _compactionLevel;

        private double _compactionScore;
        public FileMetaData FileToCompact { get; private set; }
        public int FileToCompactLevel { get; private set; }

        public Version(VersionSet versionSet)
        {
            _versionSet = versionSet;
            Preconditions.CheckArgument(NumLevels > 1, "levels must be at least 2");

            _level0 =
                new Level0(new List<FileMetaData>(), TableCache, InternalKeyComparator);

            var builder = ImmutableList.CreateBuilder<Level>();
            for (var i = 1; i < NumLevels; i++)
            {
                var files = new List<FileMetaData>();
                builder.Add(new Level(i, files, TableCache, InternalKeyComparator));
            }
            _levels = builder.ToImmutable();
        }

        public void AssertNoOverlappingFiles()
        {
            for (var level = 1; level < NumLevels; level++)
            {
                AssertNoOverlappingFiles(level);
            }
        }

        public void AssertNoOverlappingFiles(int level)
        {
            if (level > 0)
            {
                var files = GetFiles()[level];
                if (files != null)
                {
                    long previousFileNumber = 0;
                    InternalKey previousEnd = null;
                    foreach (FileMetaData fileMetaData in files)
                    {
                        if (previousEnd != null)
                        {
                            Preconditions.CheckArgument(InternalKeyComparator
                                                            .Compare(
                                                                previousEnd,
                                                                fileMetaData.Smallest
                                                            ) < 0, "Overlapping files %s and %s in level %s",
                                previousFileNumber, fileMetaData.Number, level);
                        }

                        previousFileNumber = fileMetaData.Number;
                        previousEnd = fileMetaData.Largest;
                    }
                }
            }
        }

        public MergingIterator GetIterator()
        {
            var builder = ImmutableList.CreateBuilder<IInternalIterator>();
            builder.Add(_level0.GetLevel0Iterator());
            builder.AddRange(GetLevelIterators());
            return new MergingIterator(builder.ToImmutable(), InternalKeyComparator);
        }

        public IEnumerator<Entry<InternalKey, Slice>> GetEnumerator()
        {
            return GetIterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetIterator();
        }

        internal IList<InternalTableIterator> GetLevel0Files()
        {
            var builder = ImmutableList.CreateBuilder<InternalTableIterator>();
            foreach (var file in _level0.Files)
            {
                builder.Add(TableCache.NewIterator(file));
            }
            return builder.ToImmutable();
        }

        internal IList<LevelIterator> GetLevelIterators()
        {
            var builder = ImmutableList.CreateBuilder<LevelIterator>();
            foreach (var level in _levels)
            {
                if (level.Files.Count != 0)
                {
                    builder.Add(level.GetLevelIterator());
                }
            }
            return builder.ToImmutable();
        }

        public LookupResult Get(LookupKey key)
        {
            // We can search level-by-level since entries never hop across
            // levels.  Therefore we are guaranteed that if we find data
            // in an smaller level, later levels are irrelevant.
            var readStats = new ReadStats();
            var lookupResult = _level0.Get(key, readStats);
            if (lookupResult == null)
            {
                foreach (var level in _levels)
                {
                    lookupResult = level.Get(key, readStats);
                    if (lookupResult != null)
                    {
                        break;
                    }
                }
            }
            UpdateStats(readStats.SeekFileLevel, readStats.SeekFile);
            return lookupResult;
        }

        internal int PickLevelForMemTableOutput(Slice smallestUserKey, Slice largestUserKey)
        {
            var level = 0;
            if (!OverlapInLevel(0, smallestUserKey, largestUserKey))
            {
                // Push to next level if there is no overlap in next level,
                // and the #bytes overlapping in the level after that are limited.
                var start = new InternalKey(smallestUserKey, SequenceNumber.MaxSequenceNumber, ValueType.Value);
                var limit = new InternalKey(largestUserKey, 0, ValueType.Value);
                while (level < MaxMemCompactLevel)
                {
                    if (OverlapInLevel(level + 1, smallestUserKey, largestUserKey))
                    {
                        break;
                    }
                    var sum = Compaction.TotalFileSize(_versionSet.GetOverlappingInputs(level + 2, start, limit));
                    if (sum > VersionSet.MaxGrandParentOverlapBytes)
                    {
                        break;
                    }
                    level++;
                }
            }
            return level;
        }

        public bool OverlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey)
        {
            Preconditions.CheckPositionIndex(level, _levels.Count, "Invalid level");
            Preconditions.CheckNotNull(smallestUserKey, "smallestUserKey is null");
            Preconditions.CheckNotNull(largestUserKey, "largestUserKey is null");

            return level == 0
                ? _level0.SomeFileOverlapsRange(smallestUserKey, largestUserKey)
                : _levels[level - 1].SomeFileOverlapsRange(smallestUserKey, largestUserKey);
        }

        public int NumberOfLevels()
        {
            return _levels.Count + 1;
        }

        public int NumberOfFilesInLevel(int level)
        {
            return level == 0 ? _level0.Files.Count : _levels[level - 1].Files.Count;
        }

        public MultiDictionaryBase<int, FileMetaData> GetFiles()
        {
            var mdic = new MultiDictionary<int, FileMetaData>();
            mdic.Add(0, _level0.Files);

            foreach (var level in _levels)
            {
                mdic.Add(level.LevelNumber, level.Files);
            }
            return mdic.ToImmutable();
        }

        public List<FileMetaData> GetFiles(int level)
        {
            return level == 0 ? _level0.Files : _levels[level - 1].Files;
        }

        public void AddFile(int level, FileMetaData fileMetaData)
        {
            if (level == 0)
            {
                _level0.AddFile(fileMetaData);
            }
            else
            {
                _levels[level - 1].AddFile(fileMetaData);
            }
        }

        private bool UpdateStats(int seekFileLevel, FileMetaData seekFile)
        {
            if (seekFile == null)
            {
                return false;
            }

            seekFile.DecrementAllowedSeeks();
            if (seekFile.AllowedSeeks > 0 || FileToCompact != null)
            {
                return false;
            }
            FileToCompact = seekFile;
            FileToCompactLevel = seekFileLevel;
            return true;
        }

        public long GetApproximateOffsetOf(InternalKey key)
        {
            long result = 0;
            for (int level = 0; level < NumLevels; level++)
            {
                foreach (FileMetaData fileMetaData in GetFiles(level))
                {
                    if (InternalKeyComparator.Compare(fileMetaData.Largest, key) <= 0)
                    {
                        // Entire file is before "ikey", so just add the file size
                        result += fileMetaData.FileSize;
                    }
                    else if (InternalKeyComparator.Compare(fileMetaData.Smallest, key) > 0)
                    {
                        // Entire file is after "ikey", so ignore
                        if (level > 0)
                        {
                            // Files other than level 0 are sorted by meta.smallest, so
                            // no further files in this level will contain data for
                            // "ikey".
                            break;
                        }
                    }
                    else
                    {
                        // "ikey" falls in the range for this table.  Add the
                        // approximate offset of "ikey" within the table.
                        result += TableCache.GetApproximateOffsetOf(fileMetaData, key.Encode());
                    }
                }
            }
            return result;
        }

        public void Retain()
        {
            var was = _retained;
            Interlocked.Increment(ref _retained);
            Debug.Assert(was > 0, "Version was retain after it was disposed.");
        }

        public void Release()
        {
            var now = _retained;
            Interlocked.Decrement(ref _retained);
            Debug.Assert(now >= 0, "Version was released after it was disposed.");
            if (now == 0)
            {
                // The version is now disposed.
                _versionSet.RemoveVersion(this);
            }
        }

        public bool IsDisposed()
        {
            return _retained <= 0;
        }
    }
}