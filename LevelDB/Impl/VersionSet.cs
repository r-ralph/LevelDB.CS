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
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class VersionSet : ISeekingIterable<InternalKey, Slice>
    {
        private const int L0CompactionTrigger = 4;

        public const int TargetFileSize = 2 * 1048576;

        // Maximum bytes of overlaps in grandparent (i.e., level+2) before we
        // stop building a single file in a level.level+1 compaction.
        public const long MaxGrandParentOverlapBytes = 10 * TargetFileSize;

        public InternalKeyComparator InternalKeyComparator { get; }
        public TableCache TableCache { get; }

        public Version Current { get; private set; }
        public long ManifestFileNumber { get; private set; } = 1;

        public long NextFileNumber
        {
            get
            {
                var prevValue = Interlocked.Read(ref _nextFileNumber);
                Interlocked.Increment(ref _nextFileNumber);
                return prevValue;
            }
        }

        public long PrevLogNumber { get; private set; }
        public long LogNumber { get; private set; }

        public long LastSequence
        {
            get => _lastSequence;
            set
            {
                Preconditions.CheckArgument(value >= _lastSequence,
                    "Expected newLastSequence to be greater than or equal to current lastSequence");
                _lastSequence = value;
            }
        }

        private long _nextFileNumber = 2;
        private long _lastSequence;

        private readonly WeakKeyDictionary<Version, object> _activeVersions = new WeakKeyDictionary<Version, object>();
        private readonly DirectoryInfo _databaseDir;

        private ILogWriter _descriptorLog;
        private readonly SortedDictionary<int, InternalKey> _compactPointers = new SortedDictionary<int, InternalKey>();


        public VersionSet(DirectoryInfo databaseDir, TableCache tableCache, InternalKeyComparator internalKeyComparator)
        {
            _databaseDir = databaseDir;
            TableCache = tableCache;
            InternalKeyComparator = internalKeyComparator;
            AppendVersion(new Version(this));

            InitializeIfNeeded();
        }

        private void InitializeIfNeeded()
        {
            var currentFile = new FileInfo(Path.Combine(_databaseDir.FullName, Filename.CurrentFileName()));

            if (!currentFile.Exists)
            {
                var edit = new VersionEdit
                {
                    ComparatorName = InternalKeyComparator.Name,
                    LogNumber = PrevLogNumber,
                    NextFileNumber = Interlocked.Read(ref _nextFileNumber),
                    LastSequenceNumber = LastSequence
                };

                var log = Logs.CreateLogWriter(
                    new FileInfo(Path.Combine(_databaseDir.FullName, Filename.DescriptorFileName(ManifestFileNumber))),
                    ManifestFileNumber);
                try
                {
                    WriteSnapshot(log);
                    log.AddRecord(edit.Encode(), false);
                }
                finally
                {
                    log.Close();
                }

                Filename.SetCurrentFile(_databaseDir, log.FileNumber);
            }
        }

        public void Destroy()
        {
            if (_descriptorLog != null)
            {
                _descriptorLog.Close();
                _descriptorLog = null;
            }

            var t = Current;
            if (t != null)
            {
                Current = null;
                t.Release();
            }

            // var versions = activeVersions.Keys;
            // TODO:
            // log("DB closed with "+versions.Count+" open snapshots. This could mean your application has a resource leak.");
        }

        private void AppendVersion(Version version)
        {
            Preconditions.CheckNotNull(version, "version is null");
            Preconditions.CheckArgument(version != Current, "version is the current version");
            var previous = Current;
            Current = version;
            _activeVersions.Add(version, new object());
            previous?.Release();
        }

        public void RemoveVersion(Version version)
        {
            Preconditions.CheckNotNull(version, "version is null");
            Preconditions.CheckArgument(version != Current, "version is the current version");
            var removed = !_activeVersions.Remove(version);
            Debug.Assert(removed, "Expected the version to still be in the active set");
        }

        public MergingIterator MakeInputIterator(Compaction c)
        {
            // Level-0 files have to be merged together.  For other levels,
            // we will make a concatenating iterator per level.
            // TODO(opt): use concatenating iterator for level-0 if there is no overlap
            var list = new List<IInternalIterator>();
            for (var which = 0; which < 2; which++)
            {
                if (c.Inputs[which].Count != 0)
                {
                    if (c.Level + which == 0)
                    {
                        var files = c.Inputs[which];
                        list.Add(new Level0Iterator(TableCache, files, InternalKeyComparator));
                    }
                    else
                    {
                        // Create concatenating iterator for the files from this level
                        list.Add(Level.CreateLevelConcatIterator(TableCache, c.Inputs[which], InternalKeyComparator));
                    }
                }
            }
            return new MergingIterator(list, InternalKeyComparator);
        }

        public LookupResult Get(LookupKey key)
        {
            return Current.Get(key);
        }

        public bool OverlapInLevel(int level, Slice smallestUserKey, Slice largestUserKey)
        {
            return Current.OverlapInLevel(level, smallestUserKey, largestUserKey);
        }

        public int NumberOfFilesInLevel(int level)
        {
            return Current.NumberOfFilesInLevel(level);
        }

        public long NumberOfBytesInLevel(int level)
        {
            return Current.NumberOfFilesInLevel(level);
        }

        public void LogAndApply(VersionEdit edit)
        {
            if (edit.LogNumber != null)
            {
                Preconditions.CheckArgument(edit.LogNumber >= LogNumber);
                Preconditions.CheckArgument(edit.LogNumber < Interlocked.Read(ref _nextFileNumber));
            }
            else
            {
                edit.LogNumber = LogNumber;
            }

            if (edit.PreviousLogNumber == null)
            {
                edit.PreviousLogNumber = PrevLogNumber;
            }

            edit.NextFileNumber = Interlocked.Read(ref _nextFileNumber);
            edit.LastSequenceNumber = LastSequence;

            var version = new Version(this);
            var builder = new Builder(this, Current);
            builder.Apply(edit);
            builder.SaveTo(version);

            FinalizeVersion(version);

            var createdNewManifest = false;
            try
            {
                // Initialize new descriptor log file if necessary by creating
                // a temporary file that contains a snapshot of the current version.
                if (_descriptorLog == null)
                {
                    edit.NextFileNumber = Interlocked.Read(ref _nextFileNumber);
                    _descriptorLog = Logs.CreateLogWriter(
                        new FileInfo(
                            Path.Combine(_databaseDir.FullName, Filename.DescriptorFileName(ManifestFileNumber))),
                        ManifestFileNumber);
                    WriteSnapshot(_descriptorLog);
                    createdNewManifest = true;
                }

                // Write new record to MANIFEST log
                var record = edit.Encode();
                _descriptorLog.AddRecord(record, true);

                // If we just created a new descriptor file, install it by writing a
                // new CURRENT file that points to it.
                if (createdNewManifest)
                {
                    Filename.SetCurrentFile(_databaseDir, _descriptorLog.FileNumber);
                }
            }
            catch (IOException)
            {
                // New manifest file was not installed, so clean up state and delete the file
                if (createdNewManifest)
                {
                    _descriptorLog.Close();
                    // todo add delete method to LogWriter
                    new FileInfo(Path.Combine(_databaseDir.FullName, Filename.LogFileName(_descriptorLog.FileNumber)))
                        .Delete();
                    _descriptorLog = null;
                }
                throw;
            }

            // Install the new version
            AppendVersion(version);
            Debug.Assert(edit.LogNumber != null, "edit.LogNumber != null");
            LogNumber = edit.LogNumber.Value;
            Debug.Assert(edit.PreviousLogNumber != null, "edit.PreviousLogNumber != null");
            PrevLogNumber = edit.PreviousLogNumber.Value;
        }

        private void WriteSnapshot(ILogWriter log)
        {
            // Save metadata
            var edit = new VersionEdit();
            edit.ComparatorName = InternalKeyComparator.Name;

            // Save compaction pointers
            edit.SetCompactPointers(_compactPointers);

            // Save files
            edit.AddFiles(Current.GetFiles());

            var record = edit.Encode();
            log.AddRecord(record, false);
        }

        public void Recover()
        {
            // Read "CURRENT" file, which contains a pointer to the current manifest file
            var currentFile = new FileInfo(Path.Combine(_databaseDir.FullName, Filename.CurrentFileName()));
            Preconditions.CheckState(currentFile.Exists, "CURRENT file does not exist");

            var currentName = File.ReadAllText(currentFile.FullName, Encoding.UTF8);
            if (currentName.Length == 0 || currentName[currentName.Length - 1] != '\n')
            {
                throw new InvalidOperationException("CURRENT file does not end with newline");
            }
            currentName = currentName.Substring(0, currentName.Length - 1);

            // open file channel
            using (var fileStream = new FileInfo(Path.Combine(_databaseDir.FullName, currentName)).OpenRead())
            {
                // read log edit log
                long? nextFileNumber = null;
                long? lastSequence = null;
                long? logNumber = null;
                long? prevLogNumber = null;
                var builder = new Builder(this, Current);

                var reader = new LogReader(fileStream, LogMonitors.ThrowExceptionMonitor(), true, 0);
                for (var record = reader.ReadRecord(); record != null; record = reader.ReadRecord())
                {
                    // read version edit
                    var edit = new VersionEdit(record);

                    // verify comparator
                    // todo implement user comparator
                    var editComparator = edit.ComparatorName;
                    var userComparator = InternalKeyComparator.Name;
                    Preconditions.CheckArgument(editComparator == null || editComparator.Equals(userComparator),
                        "Expected user comparator %s to match existing database comparator ", userComparator,
                        editComparator);

                    // apply edit
                    builder.Apply(edit);

                    // save edit values for verification below
                    logNumber = Coalesce(edit.LogNumber, logNumber);
                    prevLogNumber = Coalesce(edit.PreviousLogNumber, prevLogNumber);
                    nextFileNumber = Coalesce(edit.NextFileNumber, nextFileNumber);
                    lastSequence = Coalesce(edit.LastSequenceNumber, lastSequence);
                }

                var problems = new List<string>();
                if (nextFileNumber == null)
                {
                    problems.Add("Descriptor does not contain a meta-nextfile entry");
                }
                if (logNumber == null)
                {
                    problems.Add("Descriptor does not contain a meta-lognumber entry");
                }
                if (lastSequence == null)
                {
                    problems.Add("Descriptor does not contain a last-sequence-number entry");
                }
                if (problems.Count != 0)
                {
                    throw new Exception("Corruption: \n\t" + string.Join("\n\t", problems));
                }

                if (prevLogNumber == null)
                {
                    prevLogNumber = 0L;
                }

                var newVersion = new Version(this);
                builder.SaveTo(newVersion);

                // Install recovered version
                FinalizeVersion(newVersion);

                AppendVersion(newVersion);
                Debug.Assert(nextFileNumber != null, "nextFileNumber != null");
                Debug.Assert(lastSequence != null, "lastSequence != null");
                Debug.Assert(logNumber != null, "logNumber != null");
                ManifestFileNumber = nextFileNumber.Value;
                Interlocked.Exchange(ref _nextFileNumber, nextFileNumber.Value + 1);
                _lastSequence = lastSequence.Value;
                LogNumber = logNumber.Value;
                PrevLogNumber = prevLogNumber.Value;
            }
        }

        private void FinalizeVersion(Version version)
        {
            // Precomputed best level for next compaction
            var bestLevel = -1;
            double bestScore = -1;

            for (var level = 0; level < version.NumberOfLevels() - 1; level++)
            {
                double score;
                if (level == 0)
                {
                    // We treat level-0 specially by bounding the number of files
                    // instead of number of bytes for two reasons:
                    //
                    // (1) With larger write-buffer sizes, it is nice not to do too
                    // many level-0 compactions.
                    //
                    // (2) The files in level-0 are merged on every read and
                    // therefore we wish to avoid too many files when the individual
                    // file size is small (perhaps because of a small write-buffer
                    // setting, or very high compression ratios, or lots of
                    // overwrites/deletions).
                    score = 1.0 * version.NumberOfFilesInLevel(level) / L0CompactionTrigger;
                }
                else
                {
                    // Compute the ratio of current size to size limit.
                    var levelBytes = version.GetFiles(level).Sum(fileMetaData => fileMetaData.FileSize);
                    score = 1.0 * levelBytes / MaxBytesForLevel(level);
                }

                if (score > bestScore)
                {
                    bestLevel = level;
                    bestScore = score;
                }
            }

            version.CompactionLevel = bestLevel;
            version.CompactionScore = bestScore;
        }

        private static TV Coalesce<TV>(params TV[] values)
        {
            foreach (var value in values)
            {
                if (value != null)
                {
                    return value;
                }
            }
            return default(TV);
        }

        public IList<FileMetaData> GetLiveFiles()
        {
            var builder = ImmutableList.CreateBuilder<FileMetaData>();
            foreach (var activeVersion in _activeVersions.Keys)
            {
                foreach (var file in activeVersion.GetFiles())
                {
                    builder.AddRange(file.Value);
                }
            }
            return builder.ToImmutable();
        }

        private static double MaxBytesForLevel(int level)
        {
            // Note: the result for level zero is not really used since we set
            // the level-0 compaction threshold based on number of files.
            var result = 10 * 1048576.0; // Result for both level-0 and level-1
            while (level > 1)
            {
                result *= 10;
                level--;
            }
            return result;
        }

        public static long MaxFileSizeForLevel(int level)
        {
            return TargetFileSize; // We could vary per level to reduce number of files?
        }

        public bool NeedsCompaction()
        {
            return Current.CompactionScore >= 1 || Current.FileToCompact != null;
        }

        public Compaction CompactRange(int level, InternalKey begin, InternalKey end)
        {
            var levelInputs = GetOverlappingInputs(level, begin, end);
            return levelInputs.Count == 0 ? null : SetupOtherInputs(level, levelInputs);
        }

        public Compaction PickCompaction()
        {
            // We prefer compactions triggered by too much data in a level over
            // the compactions triggered by seeks.
            var sizeCompaction = Current.CompactionScore >= 1;
            var seekCompaction = Current.FileToCompact != null;

            int level;
            IList<FileMetaData> levelInputs;
            if (sizeCompaction)
            {
                level = Current.CompactionLevel;
                Preconditions.CheckState(level >= 0);
                Preconditions.CheckState(level + 1 < DbConstants.NumLevels);

                // Pick the first file that comes after compact_pointer_[level]
                levelInputs = new List<FileMetaData>();
                foreach (var fileMetaData in Current.GetFiles(level))
                {
                    if (!_compactPointers.ContainsKey(level) ||
                        InternalKeyComparator.Compare(fileMetaData.Largest, _compactPointers[level]) > 0)
                    {
                        levelInputs.Add(fileMetaData);
                        break;
                    }
                }
                if (levelInputs.Count == 0)
                {
                    // Wrap-around to the beginning of the key space
                    levelInputs.Add(Current.GetFiles(level)[0]);
                }
            }
            else if (seekCompaction)
            {
                level = Current.FileToCompactLevel;
                levelInputs = ImmutableList.Create(Current.FileToCompact);
            }
            else
            {
                return null;
            }

            // Files in level 0 may overlap each other, so pick up all overlapping ones
            if (level == 0)
            {
                var range = GetRange(levelInputs);
                // Note that the next call will discard the file we placed in
                // c->inputs_[0] earlier and replace it with an overlapping set
                // which will include the picked file.
                levelInputs = GetOverlappingInputs(0, range.Key, range.Value);

                Preconditions.CheckState(levelInputs.Count != 0);
            }

            var compaction = SetupOtherInputs(level, levelInputs);
            return compaction;
        }

        private Compaction SetupOtherInputs(int level, IList<FileMetaData> levelInputs)
        {
            var range = GetRange(levelInputs);
            var smallest = range.Key;
            var largest = range.Value;

            var levelUpInputs = GetOverlappingInputs(level + 1, smallest, largest);

            // Get entire range covered by compaction
            range = GetRange(levelInputs, levelUpInputs);
            var allStart = range.Key;
            var allLimit = range.Value;

            // See if we can grow the number of inputs in "level" without
            // changing the number of "level+1" files we pick up.
            if (levelUpInputs.Count != 0)
            {
                var expanded0 = GetOverlappingInputs(level, allStart, allLimit);

                if (expanded0.Count > levelInputs.Count)
                {
                    range = GetRange(expanded0);
                    var newStart = range.Key;
                    var newLimit = range.Value;

                    var expanded1 = GetOverlappingInputs(level + 1, newStart, newLimit);
                    if (expanded1.Count == levelUpInputs.Count)
                    {
                        //Log(options_->info_log,
                        //    "Expanding@%d %d+%d to %d+%d\n",
                        //    level,
                        //    int(c->inputs_[0].size()),
                        //    int(c->inputs_[1].size()),
                        //    int(expanded0.size()),
                        //    int(expanded1.size()));
                        smallest = newStart;
                        largest = newLimit;
                        levelInputs = expanded0;
                        levelUpInputs = expanded1;

                        range = GetRange(levelInputs, levelUpInputs);
                        allStart = range.Key;
                        allLimit = range.Value;
                    }
                }
            }

            // Compute the set of grandparent files that overlap this compaction
            // (parent == level+1; grandparent == level+2)
            IList<FileMetaData> grandparents = null;
            if (level + 2 < DbConstants.NumLevels)
            {
                grandparents = GetOverlappingInputs(level + 2, allStart, allLimit);
            }

//        if (false) {
//            Log(options_ - > info_log, "Compacting %d '%s' .. '%s'",
//                    level,
//                    EscapeString(smallest.Encode()).c_str(),
//                    EscapeString(largest.Encode()).c_str());
//        }

            var compaction = new Compaction(Current, level, levelInputs, levelUpInputs, grandparents);

            // Update the place where we will do the next compaction for this level.
            // We update this immediately instead of waiting for the VersionEdit
            // to be applied so that if the compaction fails, we will try a different
            // key range next time.
            _compactPointers[level] = largest;
            compaction.Edit.SetCompactPointer(level, largest);

            return compaction;
        }

        internal IList<FileMetaData> GetOverlappingInputs(int level, InternalKey begin, InternalKey end)
        {
            var files = ImmutableList.CreateBuilder<FileMetaData>();
            var userBegin = begin.UserKey;
            var userEnd = end.UserKey;
            var userComparator = InternalKeyComparator.UserComparator;
            foreach (var fileMetaData in Current.GetFiles(level))
            {
                if (userComparator.Compare(fileMetaData.Largest.UserKey, userBegin) < 0 ||
                    userComparator.Compare(fileMetaData.Smallest.UserKey, userEnd) > 0)
                {
                    // Either completely before or after range; skip it
                }
                else
                {
                    files.Add(fileMetaData);
                }
            }
            return files.ToImmutable();
        }

        private Entry<InternalKey, InternalKey> GetRange(params IList<FileMetaData>[] inputLists)
        {
            InternalKey smallest = null;
            InternalKey largest = null;
            foreach (var inputList in inputLists)
            {
                foreach (var fileMetaData in inputList)
                {
                    if (smallest == null)
                    {
                        smallest = fileMetaData.Smallest;
                        largest = fileMetaData.Largest;
                    }
                    else
                    {
                        if (InternalKeyComparator.Compare(fileMetaData.Smallest, smallest) < 0)
                        {
                            smallest = fileMetaData.Smallest;
                        }
                        if (InternalKeyComparator.Compare(fileMetaData.Largest, largest) > 0)
                        {
                            largest = fileMetaData.Largest;
                        }
                    }
                }
            }
            return new ImmutableEntry<InternalKey, InternalKey>(smallest, largest);
        }

        public long GetMaxNextLevelOverlappingBytes()
        {
            long result = 0;
            for (var level = 1; level < DbConstants.NumLevels; level++)
            {
                foreach (var fileMetaData in Current.GetFiles(level))
                {
                    var overlaps =
                        GetOverlappingInputs(level + 1, fileMetaData.Smallest, fileMetaData.Largest);
                    long totalSize = overlaps.Sum(overlap => overlap.FileSize);
                    result = Math.Max(result, totalSize);
                }
            }
            return result;
        }

        public MergingIterator GetIterator()
        {
            return Current.GetIterator();
        }

        public IEnumerator<Entry<InternalKey, Slice>> GetEnumerator()
        {
            return GetIterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <summary>
        /// A helper class so we can efficiently apply a whole sequence
        /// of edits to a particular state without creating intermediate
        /// Versions that contain full copies of the intermediate state.
        /// </summary>
        private class Builder
        {
            private readonly VersionSet _versionSet;
            private readonly Version _baseVersion;
            private readonly IList<LevelState> _levels;

            internal Builder(VersionSet versionSet, Version baseVersion)
            {
                _versionSet = versionSet;
                _baseVersion = baseVersion;

                _levels = new List<LevelState>(baseVersion.NumberOfLevels());
                for (var i = 0; i < baseVersion.NumberOfLevels(); i++)
                {
                    _levels.Add(new LevelState(versionSet.InternalKeyComparator));
                }
            }

            public void Apply(VersionEdit edit)
            {
                // Update compaction pointers
                foreach (var entry in edit.GetCompactPointers())
                {
                    var level = entry.Key;
                    var internalKey = entry.Value;
                    _versionSet._compactPointers.Add(level, internalKey);
                }

                // Delete files
                foreach (var entry in edit.DeletedFiles)
                {
                    var level = entry.Key;
                    foreach (var fileNumber in entry.Value)
                    {
                        _levels[level].DeletedFiles.Add(fileNumber);
                        // todo missing update to addedFiles?
                    }
                }

                // Add new files
                foreach (var entry in edit.NewFiles)
                {
                    var level = entry.Key;
                    foreach (var fileMetaData in entry.Value)
                    {
                        // We arrange to automatically compact this file after
                        // a certain number of seeks.  Let's assume:
                        //   (1) One seek costs 10ms
                        //   (2) Writing or reading 1MB costs 10ms (100MB/s)
                        //   (3) A compaction of 1MB does 25MB of IO:
                        //         1MB read from this level
                        //         10-12MB read from next level (boundaries may be misaligned)
                        //         10-12MB written to next level
                        // This implies that 25 seeks cost the same as the compaction
                        // of 1MB of data.  I.e., one seek costs approximately the
                        // same as the compaction of 40KB of data.  We are a little
                        // conservative and allow approximately one seek for every 16KB
                        // of data before triggering a compaction.
                        var allowedSeeks = (int) (fileMetaData.FileSize / 16384);
                        if (allowedSeeks < 100)
                        {
                            allowedSeeks = 100;
                        }
                        fileMetaData.AllowedSeeks = allowedSeeks;

                        _levels[level].DeletedFiles.Remove(fileMetaData.Number);
                        _levels[level].AddedFiles.Add(fileMetaData);
                    }
                }
            }

            public void SaveTo(Version version)
            {
                var cmp = new FileMetaDataBySmallestKey(_versionSet.InternalKeyComparator);

                for (var level = 0; level < _baseVersion.NumberOfLevels(); level++)
                {
                    // Merge the set of added files with the set of pre-existing files.
                    // Drop any deleted files.  Store the result in *v.

                    var baseFiles = _baseVersion.GetFiles()[level] ??
                                    (IList<FileMetaData>) ImmutableList.Create<FileMetaData>();
                    var addedFiles = _levels[level].AddedFiles ?? new SortedSet<FileMetaData>();

                    // files must be added in sorted order so assertion check in maybeAddFile works
                    var sortedFiles = new List<FileMetaData>(baseFiles.Count + addedFiles.Count);
                    sortedFiles.AddRange(baseFiles);
                    sortedFiles.AddRange(addedFiles);
                    sortedFiles.Sort(cmp);

                    foreach (var fileMetaData in sortedFiles)
                    {
                        MaybeAddFile(version, level, fileMetaData);
                    }

                    //#ifndef NDEBUG  todo
                    // Make sure there is no overlap in levels > 0
                    version.AssertNoOverlappingFiles();
                    //#endif
                }
            }

            private void MaybeAddFile(Version version, int level, FileMetaData fileMetaData)
            {
                if (_levels[level].DeletedFiles.Contains(fileMetaData.Number))
                {
                    // File is deleted: do nothing
                }
                else
                {
                    var files = version.GetFiles(level);
                    if (level > 0 && files.Count != 0)
                    {
                        // Must not overlap
                        var filesOverlap =
                            _versionSet.InternalKeyComparator.Compare(files[files.Count - 1].Largest,
                                fileMetaData.Smallest) >= 0;
                        if (filesOverlap)
                        {
                            // A memory compaction, while this compaction was running, resulted in a a database state that is
                            // incompatible with the compaction.  This is rare and expensive to detect while the compaction is
                            // running, so we catch here simply discard the work.
                            throw new IOException(
                                $"Compaction is obsolete: Overlapping files {files[files.Count - 1].Number} and {fileMetaData.Number} in level {level}");
                        }
                    }
                    version.AddFile(level, fileMetaData);
                }
            }

            private class FileMetaDataBySmallestKey : IComparer<FileMetaData>
            {
                private readonly InternalKeyComparator _internalKeyComparator;

                internal FileMetaDataBySmallestKey(InternalKeyComparator internalKeyComparator)
                {
                    _internalKeyComparator = internalKeyComparator;
                }

                public int Compare(FileMetaData f1, FileMetaData f2)
                {
                    if (f1 == null || f2 == null)
                    {
                        return 0;
                    }
                    return FirstNonZeroValue(
                        () => _internalKeyComparator.Compare(f1.Smallest, f2.Smallest),
                        () => f1.Number.CompareTo(f2.Number));
                }

                private static int FirstNonZeroValue(params Func<int>[] comparisons)
                {
                    return comparisons.Select(x => x()).FirstOrDefault(x => x != 0);
                }
            }

            private class LevelState
            {
                public SortedSet<FileMetaData> AddedFiles { get; }
                public ISet<long> DeletedFiles { get; } = new HashSet<long>();

                public LevelState(InternalKeyComparator internalKeyComparator)
                {
                    AddedFiles = new SortedSet<FileMetaData>(new FileMetaDataBySmallestKey(internalKeyComparator));
                }

                public override string ToString()
                {
                    return $"LevelState(addedFiles={AddedFiles}, deletedFiles={DeletedFiles})";
                }
            }
        }
    }
}