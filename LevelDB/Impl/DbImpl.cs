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
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LevelDB.Guava;
using LevelDB.Table;
using LevelDB.Util;
using LevelDB.Util.Atomic;
using LevelDB.Util.Extension;
using LevelDB.Util.Threading;
using static LevelDB.Impl.SequenceNumber;
using static LevelDB.Impl.ValueType;
using static LevelDB.Util.Slices;

namespace LevelDB.Impl
{
    public class DbImpl : DB<SeekingIteratorAdapter, WriteBatchImpl>
    {
        private readonly Options _options;
        private readonly DirectoryInfo _databaseDir;
        private readonly TableCache _tableCache;
        private readonly DbLock _dbLock;
        private readonly VersionSet _versions;

        private readonly AtomicBoolean _shuttingDown = new AtomicBoolean();

        private readonly ReentrantLock _mutex = new ReentrantLock();
        private readonly Condition _backgroundCondition;

        private readonly IList<long> _pendingOutputs = new List<long>(); // todo

        private ILogWriter _log;

        private MemTable _memTable;
        private MemTable _immutableMemTable;

        private readonly InternalKeyComparator _internalKeyComparator;

        private volatile Exception _backgroundException;

        //private readonly ExecutorService compactionExecutor;
        private Task _backgroundCompaction;

        private ManualCompaction _manualCompaction;

        public DbImpl(Options options, DirectoryInfo databaseDir)
        {
            Preconditions.CheckNotNull(options, $"{nameof(options)} is null");
            Preconditions.CheckNotNull(databaseDir, $"{nameof(databaseDir)} is null");
            _options = options;
            _databaseDir = databaseDir;

            _backgroundCondition = _mutex.NewCondition();

            //use custom comparator if set
            var comparator = options.Comparator();
            IUserComparator userComparator;
            if (comparator != null)
            {
                userComparator = new CustomUserComparator(comparator);
            }
            else
            {
                userComparator = new BytewiseComparator();
            }
            _internalKeyComparator = new InternalKeyComparator(userComparator);
            _memTable = new MemTable(_internalKeyComparator);
            _immutableMemTable = null;
            /*
            ThreadFactory compactionThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat("leveldb-compaction-%s")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandler()
                {
                    @Override

                    public void uncaughtException(Thread t,
                    Throwable e)
                    {
                    // todo need a real UncaughtExceptionHandler
                    System.out.printf("%s%n",
                    t);
                    e.printStackTrace();
                }
            })
            .build();
            compactionExecutor = Executors.newSingleThreadExecutor(compactionThreadFactory);
            */

            // Reserve ten files or so for other uses and give the rest to TableCache.
            var tableCacheSize = options.MaxOpenFiles() - 10;
            _tableCache = new TableCache(databaseDir, tableCacheSize,
                new InternalUserComparator(_internalKeyComparator),
                options.VerifyChecksums());

            // create the version set

            // create the database dir if it does not already exist
            if (!databaseDir.Exists)
            {
                databaseDir.Create();
            }
            Preconditions.CheckArgument(Directory.Exists(databaseDir.FullName),
                $"Database directory '{databaseDir.FullName}' does not exist and could not be created");
            _mutex.Lock();
            try
            {
                // lock the database dir
                _dbLock = new DbLock(new FileInfo(Path.Combine(databaseDir.FullName, Filename.LockFileName())));

                // verify the "current" file
                var currentFile = new FileInfo(Path.Combine(databaseDir.FullName, Filename.CurrentFileName()));
                if (!currentFile.IsReadable())
                {
                    Preconditions.CheckArgument(options.CreateIfMissing(),
                        "Database '%s' does not exist and the create if missing option is disabled", databaseDir);
                }
                else
                {
                    Preconditions.CheckArgument(!options.ErrorIfExists(),
                        "Database '%s' exists and the error if exists option is enabled", databaseDir);
                }

                _versions = new VersionSet(databaseDir, _tableCache, _internalKeyComparator);

                // load (and recover) current version
                _versions.Recover();

                // Recover from all newer log files than the ones named in the
                // descriptor (new log files may have been added by the previous
                // incarnation without registering them in the descriptor).
                //
                // Note that PrevLogNumber() is no longer used, but we pay
                // attention to it in case we are recovering a database
                // produced by an older version of leveldb.
                var minLogNumber = _versions.LogNumber;
                var previousLogNumber = _versions.PrevLogNumber;
                var filenames = Filename.ListFiles(databaseDir);
                var logs = (from filename in filenames
                        select Filename.ParseFileName(filename)
                        into fileInfo
                        where fileInfo != null && fileInfo.FileType == Filename.FileType.Log &&
                              (fileInfo.FileNumber >= minLogNumber || fileInfo.FileNumber == previousLogNumber)
                        select fileInfo.FileNumber)
                    .ToList();

                // Recover in the order in which the logs were generated
                var edit = new VersionEdit();
                logs.Sort();
                foreach (var fileNumber in logs)
                {
                    var maxSequence = RecoverLogFile(fileNumber, edit);
                    if (_versions.LastSequence < maxSequence)
                    {
                        _versions.LastSequence = maxSequence;
                    }
                }

                // open transaction log
                var logFileNumber = _versions.NextFileNumber;
                _log = Logs.CreateLogWriter(
                    new FileInfo(Path.Combine(databaseDir.FullName, Filename.LogFileName(logFileNumber))),
                    logFileNumber);
                edit.LogNumber = _log.FileNumber;

                // apply recovered edits
                _versions.LogAndApply(edit);

                // cleanup unused files
                DeleteObsoleteFiles();

                // schedule compactions
                MaybeScheduleCompaction();
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        public void Dispose()
        {
            if (_shuttingDown.GetAndSet(true))
            {
                return;
            }

            _mutex.Lock();
            try
            {
                while (_backgroundCompaction != null)
                {
                    _backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                _mutex.Unlock();
            }

            /*
            compactionExecutor.shutdown();
            try
            {
                compactionExecutor.awaitTermination(1, TimeUnit.DAYS);
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
            */
            try
            {
                _versions.Destroy();
            }
            catch (IOException)
            {
            }
            try
            {
                _log.Close();
            }
            catch (IOException)
            {
            }
            _tableCache.Close();
            _dbLock.Release();
        }

        public string GetProperty(string name)
        {
            CheckBackgroundException();
            return null;
        }

        private void DeleteObsoleteFiles()
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            // Make a set of all of the live files
            var live = new List<long>(_pendingOutputs);
            foreach (var fileMetaData in _versions.GetLiveFiles())
            {
                live.Add(fileMetaData.Number);
            }

            foreach (var file in Filename.ListFiles(_databaseDir))
            {
                var fileInfo = Filename.ParseFileName(file);
                if (fileInfo == null)
                {
                    continue;
                }
                var number = fileInfo.FileNumber;
                var keep = true;
                switch (fileInfo.FileType)
                {
                    case Filename.FileType.Log:
                        keep = ((number >= _versions.LogNumber) ||
                                (number == _versions.PrevLogNumber));
                        break;
                    case Filename.FileType.Descriptor:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= _versions.ManifestFileNumber);
                        break;
                    case Filename.FileType.Table:
                        keep = live.Contains(number);
                        break;
                    case Filename.FileType.Temp:
                        // Any temp files that are currently being written to must
                        // be recorded in pending_outputs_, which is inserted into "live"
                        keep = live.Contains(number);
                        break;
                    case Filename.FileType.Current:
                    case Filename.FileType.DBLock:
                    case Filename.FileType.InfoLog:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                if (!keep)
                {
                    if (fileInfo.FileType == Filename.FileType.Table)
                    {
                        _tableCache.Evict(number);
                    }
                    // todo info logging system needed
                    // Log(options_.info_log, "Delete type=%d #%lld\n",
                    // int(type),
                    // static_cast < unsigned long long>(number));
                    file.Delete();
                }
            }
        }

        public void FlushMemTable()
        {
            _mutex.Lock();
            try
            {
                // force compaction
                MakeRoomForWrite(true);

                // todo bg_error code
                while (_immutableMemTable != null)
                {
                    _backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        public void CompactRange(int level, Slice start, Slice end)
        {
            Preconditions.CheckArgument(level >= 0, "level is negative");
            Preconditions.CheckArgument(level + 1 < DbConstants.NumLevels,
                $"level is greater than or equal to {DbConstants.NumLevels}");
            Preconditions.CheckNotNull(start, "start is null");
            Preconditions.CheckNotNull(end, "end is null");

            _mutex.Lock();
            try
            {
                while (_manualCompaction != null)
                {
                    _backgroundCondition.AwaitUninterruptibly();
                }
                var manualCompaction = new ManualCompaction(level, start, end);
                _manualCompaction = manualCompaction;

                MaybeScheduleCompaction();

                while (_manualCompaction == manualCompaction)
                {
                    _backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        private void MaybeScheduleCompaction()
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            if (_backgroundCompaction != null)
            {
                // Already scheduled
            }
            else if (_shuttingDown.Value)
            {
                // DB is being shutdown; no more background compactions
            }
            else if (_immutableMemTable == null &&
                     _manualCompaction == null &&
                     !_versions.NeedsCompaction())
            {
                // No work to be done
            }
            else
            {
                _backgroundCompaction = Task.Factory.StartNew(() =>
                {
                    try
                    {
                        BackgroundCall();
                    }
                    catch (DatabaseShutdownException)
                    {
                    }
                    catch (Exception e)
                    {
                        _backgroundException = e;
                    }
                });
            }
        }

        public void CheckBackgroundException()
        {
            var e = _backgroundException;
            if (e != null)
            {
                throw new BackgroundProcessingException(e);
            }
        }

        private void BackgroundCall()
        {
            _mutex.Lock();
            try
            {
                if (_backgroundCompaction == null)
                {
                    return;
                }

                try
                {
                    if (!_shuttingDown.Value)
                    {
                        BackgroundCompaction();
                    }
                }
                finally
                {
                    _backgroundCompaction = null;
                }
            }
            finally
            {
                try
                {
                    // Previous compaction may have produced too many files in a level,
                    // so reschedule another compaction if needed.
                    MaybeScheduleCompaction();
                }
                finally
                {
                    try
                    {
                        _backgroundCondition.SignalAll();
                    }
                    finally
                    {
                        _mutex.Unlock();
                    }
                }
            }
        }

        private void BackgroundCompaction()
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            CompactMemTableInternal();

            Compaction compaction;
            if (_manualCompaction != null)
            {
                compaction = _versions.CompactRange(_manualCompaction.Level,
                    new InternalKey(_manualCompaction.Begin, MaxSequenceNumber, Value),
                    new InternalKey(_manualCompaction.End, 0, Deletion));
            }
            else
            {
                compaction = _versions.PickCompaction();
            }

            if (compaction == null)
            {
                // no compaction
            }
            else if (_manualCompaction == null && compaction.IsTrivialMove())
            {
                // Move file to next level
                Preconditions.CheckState(compaction.LevelInputs.Count == 1);
                var fileMetaData = compaction.LevelInputs[0];
                compaction.Edit.DeleteFile(compaction.Level, fileMetaData.Number);
                compaction.Edit.AddFile(compaction.Level + 1, fileMetaData);
                _versions.LogAndApply(compaction.Edit);
                // log
            }
            else
            {
                var compactionState = new CompactionState(compaction);
                DoCompactionWork(compactionState);
                CleanupCompaction(compactionState);
            }

            // manual compaction complete
            _manualCompaction = null;
        }

        private void CleanupCompaction(CompactionState compactionState)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            if (compactionState.Builder != null)
            {
                compactionState.Builder.Abandon();
            }
            else
            {
                Preconditions.CheckArgument(compactionState.Outfile == null);
            }

            foreach (var output in compactionState.Outputs)
            {
                _pendingOutputs.Remove(output.Number);
            }
        }

        private long RecoverLogFile(long fileNumber, VersionEdit edit)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());
            var file = new FileInfo(Path.Combine(_databaseDir.FullName, Filename.LogFileName(fileNumber)));
            using (var stream = file.Open(FileMode.OpenOrCreate))
            {
                var logMonitor = LogMonitors.LogMonitor();
                var logReader = new LogReader(stream, logMonitor, true, 0);

                // Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);

                // Read all the records and add to a memtable
                long maxSequence = 0;
                MemTable memTable = null;
                for (var record = logReader.ReadRecord(); record != null; record = logReader.ReadRecord())
                {
                    var sliceInput = record.Input();
                    // read header
                    if (sliceInput.Available < 12)
                    {
                        logMonitor.Corruption(sliceInput.Available, "log record too small");
                        continue;
                    }
                    var sequenceBegin = sliceInput.ReadLong();
                    var updateSize = sliceInput.ReadInt();

                    // read entries
                    var writeBatch = ReadWriteBatch(sliceInput, updateSize);

                    // apply entries to memTable
                    if (memTable == null)
                    {
                        memTable = new MemTable(_internalKeyComparator);
                    }
                    writeBatch.ForEach(new InsertIntoHandler(memTable, sequenceBegin));

                    // update the maxSequence
                    var lastSequence = sequenceBegin + updateSize - 1;
                    if (lastSequence > maxSequence)
                    {
                        maxSequence = lastSequence;
                    }

                    // flush mem table if necessary
                    if (memTable.ApproximateMemoryUsage > _options.WriteBufferSize())
                    {
                        WriteLevel0Table(memTable, edit, null);
                        memTable = null;
                    }
                }

                // flush mem table
                if (memTable != null && !memTable.IsEmpty)
                {
                    WriteLevel0Table(memTable, edit, null);
                }

                return maxSequence;
            }
        }

        public byte[] Get(byte[] key)
        {
            return Get(key, new ReadOptions());
        }

        public byte[] Get(byte[] key, ReadOptions options)
        {
            CheckBackgroundException();
            LookupKey lookupKey;
            _mutex.Lock();
            LookupResult lookupResult;
            try
            {
                var snapshot = GetSnapshot(options);
                lookupKey = new LookupKey(WrappedBuffer(key), snapshot.GetLastSequence());

                // First look in the memtable, then in the immutable memtable (if any).
                lookupResult = _memTable.Get(lookupKey);
                if (lookupResult != null)
                {
                    var value = lookupResult.Value;
                    return value?.GetBytes();
                }
                if (_immutableMemTable != null)
                {
                    lookupResult = _immutableMemTable.Get(lookupKey);
                    if (lookupResult != null)
                    {
                        var value = lookupResult.Value;
                        return value?.GetBytes();
                    }
                }
            }
            finally
            {
                _mutex.Unlock();
            }

            // Not in memTables; try live files in level order
            lookupResult = _versions.Get(lookupKey);

            // schedule compaction if necessary
            _mutex.Lock();
            try
            {
                if (_versions.NeedsCompaction())
                {
                    MaybeScheduleCompaction();
                }
            }
            finally
            {
                _mutex.Unlock();
            }

            if (lookupResult != null)
            {
                var value = lookupResult.Value;
                if (value != null)
                {
                    return value.GetBytes();
                }
            }
            return null;
        }

        public void Put(byte[] key, byte[] value)
        {
            Put(key, value, new WriteOptions());
        }

        public ISnapshot Put(byte[] key, byte[] value, WriteOptions options)
        {
            return WriteInternal(new WriteBatchImpl().Put(key, value), options);
        }

        public void Delete(byte[] key)
        {
            WriteInternal(new WriteBatchImpl().Delete(key), new WriteOptions());
        }

        public ISnapshot Delete(byte[] key, WriteOptions options)
        {
            return WriteInternal(new WriteBatchImpl().Delete(key), options);
        }

        public void Write(WriteBatchImpl updates)
        {
            WriteInternal(updates, new WriteOptions());
        }

        public ISnapshot Write(WriteBatchImpl updates, WriteOptions options)
        {
            return WriteInternal(updates, options);
        }

        public ISnapshot WriteInternal(WriteBatchImpl updates, WriteOptions options)
        {
            CheckBackgroundException();
            _mutex.Lock();
            try
            {
                long sequenceEnd;
                if (updates.Size != 0)
                {
                    MakeRoomForWrite(false);

                    // Get sequence numbers for this change set
                    var sequenceBegin = _versions.LastSequence + 1;
                    sequenceEnd = sequenceBegin + updates.Size - 1;

                    // Reserve this sequence in the version set
                    _versions.LastSequence = sequenceEnd;

                    // Log write
                    var record = WriteWriteBatch(updates, sequenceBegin);
                    _log.AddRecord(record, options.Sync());

                    // Update memtable
                    updates.ForEach(new InsertIntoHandler(_memTable, sequenceBegin));
                }
                else
                {
                    sequenceEnd = _versions.LastSequence;
                }

                return options.Snapshot() ? new SnapshotImpl(_versions.Current, sequenceEnd) : null;
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        public WriteBatchImpl CreateWriteBatch()
        {
            CheckBackgroundException();
            return new WriteBatchImpl();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator<Entry<byte[], byte[]>> IEnumerable<Entry<byte[], byte[]>>.GetEnumerator()
        {
            return GetEnumerator();
        }

        public SeekingIteratorAdapter GetEnumerator()
        {
            return GetEnumerator(new ReadOptions());
        }

        public SeekingIteratorAdapter GetEnumerator(ReadOptions options)
        {
            CheckBackgroundException();
            _mutex.Lock();
            try
            {
                var rawIterator = InternalIterator();

                // filter any entries not visible in our snapshot
                var snapshot = GetSnapshot(options);
                var snapshotIterator =
                    new SnapshotSeekingIterator(rawIterator, snapshot, _internalKeyComparator.UserComparator);
                return new SeekingIteratorAdapter(snapshotIterator);
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        public ISeekingIterable<InternalKey, Slice> InternalIterable()
        {
            return new InternalSeekingIterable(this);
        }

        private DbIterator InternalIterator()
        {
            _mutex.Lock();
            try
            {
                // merge together the memTable, immutableMemTable, and tables in version set
                MemTable.MemTableIterator iterator = null;
                if (_immutableMemTable != null)
                {
                    iterator = _immutableMemTable.GetMemTableIterator();
                }
                var current = _versions.Current;
                return new DbIterator(_memTable.GetMemTableIterator(), iterator, current.GetLevel0Files(),
                    current.GetLevelIterators(), _internalKeyComparator);
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        public ISnapshot GetSnapshot()
        {
            CheckBackgroundException();
            _mutex.Lock();
            try
            {
                return new SnapshotImpl(_versions.Current, _versions.LastSequence);
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        private SnapshotImpl GetSnapshot(ReadOptions options)
        {
            SnapshotImpl snapshot;
            if (options.Snapshot() != null)
            {
                snapshot = (SnapshotImpl) options.Snapshot();
            }
            else
            {
                snapshot = new SnapshotImpl(_versions.Current, _versions.LastSequence);
                snapshot.Dispose(); // To avoid holding the snapshot active..
            }
            return snapshot;
        }

        private void MakeRoomForWrite(bool force)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            var allowDelay = !force;

            while (true)
            {
                // todo background processing system need work
//            if (!bg_error_.ok()) {
//              // Yield previous error
//              s = bg_error_;
//              break;
//            } else
                if (allowDelay && _versions.NumberOfFilesInLevel(0) > DbConstants.L0SlowdownWritesTrigger)
                {
                    // We are getting close to hitting a hard limit on the number of
                    // L0 files.  Rather than delaying a single write by several
                    // seconds when we hit the hard limit, start delaying each
                    // individual write by 1ms to reduce latency variance.  Also,
                    // this delay hands over some CPU to the compaction thread in
                    // case it is sharing the same core as the writer.
                    try
                    {
                        _mutex.Unlock();
                        //Thread.sleep(1);
                    }
                    catch (Exception)
                    {
                        //    Thread.currentThread().interrupt();
                        //    throw new RuntimeException(e);
                    }
                    finally
                    {
                        _mutex.Lock();
                    }

                    // Do not delay a single write more than once
                    allowDelay = false;
                }
                else if (!force && _memTable.ApproximateMemoryUsage <= _options.WriteBufferSize())
                {
                    // There is room in current memtable
                    break;
                }
                else if (_immutableMemTable != null)
                {
                    // We have filled up the current memtable, but the previous
                    // one is still being compacted, so we wait.
                    _backgroundCondition.AwaitUninterruptibly();
                }
                else if (_versions.NumberOfFilesInLevel(0) >= DbConstants.L0StopWritesTrigger)
                {
                    // There are too many level-0 files.
//                Log(options_.info_log, "waiting...\n");
                    _backgroundCondition.AwaitUninterruptibly();
                }
                else
                {
                    // Attempt to switch to a new memtable and trigger compaction of old
                    Preconditions.CheckState(_versions.PrevLogNumber == 0);

                    // close the existing log
                    try
                    {
                        _log.Close();
                    }
                    catch (IOException e)
                    {
                        throw new Exception($"Unable to close log file {_log.File}", e);
                    }

                    // open a new log
                    var logNumber = _versions.NextFileNumber;
                    try
                    {
                        _log = Logs.CreateLogWriter(
                            new FileInfo(Path.Combine(_databaseDir.FullName, Filename.LogFileName(logNumber))),
                            logNumber);
                    }
                    catch (IOException e)
                    {
                        throw new Exception("Unable to open new log file " +
                                            new FileInfo(Path.Combine(_databaseDir.FullName,
                                                Filename.LogFileName(logNumber))).FullName, e);
                    }

                    // create a new mem table
                    _immutableMemTable = _memTable;
                    _memTable = new MemTable(_internalKeyComparator);

                    // Do not force another compaction there is space available
                    force = false;

                    MaybeScheduleCompaction();
                }
            }
        }

        public void CompactMemTable()
        {
            _mutex.Lock();
            try
            {
                CompactMemTableInternal();
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        private void CompactMemTableInternal()
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());
            if (_immutableMemTable == null)
            {
                return;
            }

            try
            {
                // Save the contents of the memtable as a new Table
                var edit = new VersionEdit();
                var baseVersion = _versions.Current;
                WriteLevel0Table(_immutableMemTable, edit, baseVersion);

                if (_shuttingDown.Value)
                {
                    throw new Exception("Database shutdown during memtable compaction");
                }

                // Replace immutable memtable with the generated Table
                edit.PreviousLogNumber = 0;
                edit.LogNumber = (_log.FileNumber); // Earlier logs no longer needed
                _versions.LogAndApply(edit);

                _immutableMemTable = null;

                DeleteObsoleteFiles();
            }
            finally
            {
                _backgroundCondition.SignalAll();
            }
        }


        private void WriteLevel0Table(MemTable mem, VersionEdit edit, Version baseVersion)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            // skip empty mem table
            if (mem.IsEmpty)
            {
                return;
            }

            // write the memtable to a new sstable
            var fileNumber = _versions.NextFileNumber;
            _pendingOutputs.Add(fileNumber);
            _mutex.Unlock();
            FileMetaData meta;
            try
            {
                meta = BuildTable(mem, fileNumber);
            }
            finally
            {
                _mutex.Lock();
            }
            _pendingOutputs.Remove(fileNumber);

            // Note that if file size is zero, the file has been deleted and
            // should not be added to the manifest.
            var level = 0;
            if (meta != null && meta.FileSize > 0)
            {
                var minUserKey = meta.Smallest.UserKey;
                var maxUserKey = meta.Largest.UserKey;
                if (baseVersion != null)
                {
                    level = baseVersion.PickLevelForMemTableOutput(minUserKey, maxUserKey);
                }
                edit.AddFile(level, meta);
            }
        }

        private FileMetaData BuildTable(MemTable data, long fileNumber)
        {
            var file = new FileInfo(Path.Combine(_databaseDir.FullName, Filename.TableFileName(fileNumber)));
            try
            {
                InternalKey smallest = null;
                InternalKey largest = null;
                var channel = file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                try
                {
                    var tableBuilder =
                        new TableBuilder(_options, channel, new InternalUserComparator(_internalKeyComparator));
                    var memTableIterator = data.GetMemTableIterator();
                    while (memTableIterator.HasNext())
                    {
                        var entry = memTableIterator.Next();
                        // update keys
                        var key = entry.Key;
                        if (smallest == null)
                        {
                            smallest = key;
                        }
                        largest = key;

                        tableBuilder.Add(key.Encode(), entry.Value);
                    }

                    tableBuilder.Finish();
                }
                finally
                {
                    try
                    {
                        channel.Flush(true);
                    }
                    finally
                    {
                        channel.Dispose();
                    }
                }

                if (smallest == null)
                {
                    return null;
                }
                var fileMetaData = new FileMetaData(fileNumber, file.Length, smallest, largest);

                // verify table can be opened
                _tableCache.NewIterator(fileMetaData);

                _pendingOutputs.Remove(fileNumber);

                return fileMetaData;
            }
            catch (IOException)
            {
                file.Delete();
                throw;
            }
        }

        private void DoCompactionWork(CompactionState compactionState)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());
            Preconditions.CheckArgument(_versions.NumberOfBytesInLevel(compactionState.Compaction.Level) > 0);
            Preconditions.CheckArgument(compactionState.Builder == null);
            Preconditions.CheckArgument(compactionState.Outfile == null);

            // todo track snapshots
            compactionState.SmallestSnapshot = _versions.LastSequence;

            // Release mutex while we're actually doing the compaction work
            _mutex.Unlock();
            try
            {
                var iterator = _versions.MakeInputIterator(compactionState.Compaction);

                Slice currentUserKey = null;
                var hasCurrentUserKey = false;

                var lastSequenceForKey = MaxSequenceNumber;
                while (iterator.HasNext() && !_shuttingDown.Value)
                {
                    // always give priority to compacting the current mem table
                    _mutex.Lock();
                    try
                    {
                        CompactMemTableInternal();
                    }
                    finally
                    {
                        _mutex.Unlock();
                    }

                    var key = iterator.Peek().Key;
                    if (compactionState.Compaction.ShouldStopBefore(key) && compactionState.Builder != null)
                    {
                        FinishCompactionOutputFile(compactionState);
                    }

                    // Handle key/value, add to state, etc.
                    var drop = false;
                    // todo if key doesn't parse (it is corrupted),
                    if (false /*!ParseInternalKey(key, &ikey)*/)
                    {
                        // do not hide error keys
                        currentUserKey = null;
                        hasCurrentUserKey = false;
                        lastSequenceForKey = MaxSequenceNumber;
                    }
                    else
                    {
                        if (!hasCurrentUserKey || _internalKeyComparator.UserComparator
                                .Compare(key.UserKey, currentUserKey) != 0)
                        {
                            // First occurrence of this user key
                            currentUserKey = key.UserKey;
                            hasCurrentUserKey = true;
                            lastSequenceForKey = MaxSequenceNumber;
                        }

                        if (lastSequenceForKey <= compactionState.SmallestSnapshot)
                        {
                            // Hidden by an newer entry for same user key
                            drop = true; // (A)
                        }
                        else if (key.ValueType == Deletion &&
                                 key.SequenceNumber <= compactionState.SmallestSnapshot &&
                                 compactionState.Compaction.IsBaseLevelForKey(key.UserKey))
                        {
                            // For this user key:
                            // (1) there is no data in higher levels
                            // (2) data in lower levels will have larger sequence numbers
                            // (3) data in layers that are being compacted here and have
                            //     smaller sequence numbers will be dropped in the next
                            //     few iterations of this loop (by rule (A) above).
                            // Therefore this deletion marker is obsolete and can be dropped.
                            drop = true;
                        }

                        lastSequenceForKey = key.SequenceNumber;
                    }

                    if (!drop)
                    {
                        // Open output file if necessary
                        if (compactionState.Builder == null)
                        {
                            OpenCompactionOutputFile(compactionState);
                        }
                        if (compactionState.Builder.GetEntryCount() == 0)
                        {
                            compactionState.CurrentSmallest = key;
                        }
                        compactionState.CurrentLargest = key;
                        compactionState.Builder.Add(key.Encode(), iterator.Peek().Value);

                        // Close output file if it is big enough
                        if (compactionState.Builder.GetFileSize() >=
                            compactionState.Compaction.MaxOutputFileSize)
                        {
                            FinishCompactionOutputFile(compactionState);
                        }
                    }
                    iterator.Next();
                }

                if (_shuttingDown.Value)
                {
                    throw new Exception("DB shutdown during compaction");
                }
                if (compactionState.Builder != null)
                {
                    FinishCompactionOutputFile(compactionState);
                }
            }
            finally
            {
                _mutex.Lock();
            }

            // todo port CompactionStats code

            InstallCompactionResults(compactionState);
        }

        private void OpenCompactionOutputFile(CompactionState compactionState)
        {
            Preconditions.CheckNotNull(compactionState, $"{nameof(compactionState)} is null");
            Preconditions.CheckArgument(compactionState.Builder == null,
                $"{nameof(compactionState)} builder is not null");

            _mutex.Lock();
            try
            {
                var fileNumber = _versions.NextFileNumber;
                _pendingOutputs.Add(fileNumber);
                compactionState.CurrentFileNumber = fileNumber;
                compactionState.CurrentFileSize = 0;
                compactionState.CurrentSmallest = null;
                compactionState.CurrentLargest = null;

                var file = new FileInfo(Path.Combine(_databaseDir.FullName, Filename.TableFileName(fileNumber)));
                compactionState.Outfile = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                compactionState.Builder = new TableBuilder(_options, compactionState.Outfile,
                    new InternalUserComparator(_internalKeyComparator));
            }
            finally
            {
                _mutex.Unlock();
            }
        }

        private void FinishCompactionOutputFile(CompactionState compactionState)
        {
            Preconditions.CheckNotNull(compactionState, $"{nameof(compactionState)} is null");
            Preconditions.CheckArgument(compactionState.Outfile != null);
            Preconditions.CheckArgument(compactionState.Builder != null);

            var outputNumber = compactionState.CurrentFileNumber;
            Preconditions.CheckArgument(outputNumber != 0);

            var currentEntries = compactionState.Builder.GetEntryCount();
            compactionState.Builder.Finish();

            var currentBytes = compactionState.Builder.GetFileSize();
            compactionState.CurrentFileSize = currentBytes;
            compactionState.TotalBytes += currentBytes;

            var currentFileMetaData = new FileMetaData(compactionState.CurrentFileNumber,
                compactionState.CurrentFileSize,
                compactionState.CurrentSmallest,
                compactionState.CurrentLargest);
            compactionState.Outputs.Add(currentFileMetaData);

            compactionState.Builder = null;

            //compactionState.outfile.Force(true);
            compactionState.Outfile.Dispose();
            compactionState.Outfile = null;

            if (currentEntries > 0)
            {
                // Verify that the table is usable
                _tableCache.NewIterator(outputNumber);
            }
        }

        private void InstallCompactionResults(CompactionState compact)
        {
            Preconditions.CheckState(_mutex.IsHeldByCurrentThread());

            // Add compaction outputs
            compact.Compaction.AddInputDeletions(compact.Compaction.Edit);
            var level = compact.Compaction.Level;
            foreach (var output in compact.Outputs)
            {
                compact.Compaction.Edit.AddFile(level + 1, output);
                _pendingOutputs.Remove(output.Number);
            }

            try
            {
                _versions.LogAndApply(compact.Compaction.Edit);
                DeleteObsoleteFiles();
            }
            catch (IOException e)
            {
                // Compaction failed for some reason.  Simply discard the work and try again later.

                // Discard any files we may have created during this failed compaction
                foreach (var output in compact.Outputs)
                {
                    var file = new FileInfo(
                        Path.Combine(_databaseDir.FullName, Filename.TableFileName(output.Number)));
                    file.Delete();
                }
                compact.Outputs.Clear();
            }
        }

        public int NumberOfFilesInLevel(int level)
        {
            return _versions.Current.NumberOfFilesInLevel(level);
        }

        public long[] GetApproximateSizes(params Range[] ranges)
        {
            Preconditions.CheckNotNull(ranges, $"{nameof(ranges)} is null");
            var sizes = new long[ranges.Length];
            for (var i = 0; i < ranges.Length; i++)
            {
                var range = ranges[i];
                sizes[i] = GetApproximateSizes(range);
            }
            return sizes;
        }

        public long GetApproximateSizes(Range range)
        {
            var v = _versions.Current;

            var startKey = new InternalKey(WrappedBuffer(range.Start()), MaxSequenceNumber, Value);
            var limitKey = new InternalKey(WrappedBuffer(range.Limit()), MaxSequenceNumber, Value);
            var startOffset = v.GetApproximateOffsetOf(startKey);
            var limitOffset = v.GetApproximateOffsetOf(limitKey);

            return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
        }

        public long GetMaxNextLevelOverlappingBytes()
        {
            return _versions.GetMaxNextLevelOverlappingBytes();
        }

        private WriteBatchImpl ReadWriteBatch(SliceInput record, int updateSize)
        {
            var writeBatch = new WriteBatchImpl();
            var entries = 0;
            while (record.CanRead)
            {
                entries++;
                var valueType = GetValueTypeByPersistentId(record.ReadByteAlt());
                if (valueType == Value)
                {
                    var key = ReadLengthPrefixedBytes(record);
                    var value = ReadLengthPrefixedBytes(record);
                    writeBatch.Put(key, value);
                }
                else if (valueType == Deletion)
                {
                    var key = ReadLengthPrefixedBytes(record);
                    writeBatch.Delete(key);
                }
                else
                {
                    throw new InvalidOperationException("Unexpected value type " + valueType);
                }
            }

            if (entries != updateSize)
            {
                throw new IOException($"Expected {updateSize} entries in log record but found {entries} entries");
            }

            return writeBatch;
        }

        private Slice WriteWriteBatch(WriteBatchImpl updates, long sequenceBegin)
        {
            var record = Allocate(SizeOf.Long + SizeOf.Int + updates.ApproximateSize);
            var sliceOutput = record.Output();
            sliceOutput.WriteLong(sequenceBegin);
            sliceOutput.WriteInt(updates.Size);
            updates.ForEach((key, value) =>
            {
                sliceOutput.WriteByte((byte) Value.PersistentId);
                WriteLengthPrefixedBytes(sliceOutput, key);
                WriteLengthPrefixedBytes(sliceOutput, value);
            }, key =>
            {
                sliceOutput.WriteByte((byte) Deletion.PersistentId);
                WriteLengthPrefixedBytes(sliceOutput, key);
            });

            return record.Sliced(0, sliceOutput.Size());
        }

        private readonly object _suspensionMutex = new object();
        private int _suspensionCounter;

        public void SuspendCompactions()
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;
            Task.Factory.StartNew(() =>
            {
                try
                {
                    lock (_suspensionMutex)
                    {
                        _suspensionCounter++;
                        Monitor.PulseAll(_suspensionMutex);
                        while (_suspensionCounter > 0 && !token.IsCancellationRequested)
                        {
                            Monitor.Wait(_suspensionMutex, 500);
                        }
                    }
                }
                catch (Exception)
                {
                    // ignored
                }
            }, token);

            lock (_suspensionMutex)
            {
                while (_suspensionCounter < 1)
                {
                    Monitor.Wait(_suspensionMutex);
                }
            }
        }

        public void ResumeCompactions()
        {
            lock (_suspensionMutex)
            {
                _suspensionCounter--;
                Monitor.PulseAll(_suspensionMutex);
            }
        }

        public void CompactRange(byte[] begin, byte[] end)
        {
            throw new NotImplementedException();
        }


        private class InternalSeekingIterable : ISeekingIterable<InternalKey, Slice>
        {
            private readonly DbImpl _dbImpl;

            public InternalSeekingIterable(DbImpl dbImpl)
            {
                _dbImpl = dbImpl;
            }

            public DbIterator GetDBIterator()
            {
                return _dbImpl.InternalIterator();
            }

            public IEnumerator<Entry<InternalKey, Slice>> GetEnumerator()
            {
                return GetDBIterator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
        }

        private class CompactionState
        {
            public Compaction Compaction { get; }

            internal readonly IList<FileMetaData> Outputs = new List<FileMetaData>();

            internal long SmallestSnapshot;

            // State kept for output being generated
            internal FileStream Outfile;

            internal TableBuilder Builder;

            // Current file being generated
            internal long CurrentFileNumber;

            internal long CurrentFileSize;
            internal InternalKey CurrentSmallest;
            internal InternalKey CurrentLargest;

            internal long TotalBytes;

            internal CompactionState(Compaction compaction)
            {
                Compaction = compaction;
            }
        }

        private class ManualCompaction
        {
            public int Level { get; }
            public Slice Begin { get; }
            public Slice End { get; }

            internal ManualCompaction(int level, Slice begin, Slice end)
            {
                Level = level;
                Begin = begin;
                End = end;
            }
        }

        public class DatabaseShutdownException : DBException
        {
            public DatabaseShutdownException()
            {
            }

            public DatabaseShutdownException(String message) : base(message)
            {
            }
        }

        public class BackgroundProcessingException : DBException
        {
            public BackgroundProcessingException(Exception cause) : base("", cause)
            {
            }
        }

        private class InsertIntoHandler : WriteBatchImpl.IHandler
        {
            private long _sequence;
            private readonly MemTable _memTable;

            public InsertIntoHandler(MemTable memTable, long sequenceBegin)
            {
                _memTable = memTable;
                _sequence = sequenceBegin;
            }

            public void Put(Slice key, Slice value)
            {
                _memTable.Add(_sequence++, Value, key, value);
            }

            public void Delete(Slice key)
            {
                _memTable.Add(_sequence++, Deletion, key, EmptySlice);
            }
        }
    }
}