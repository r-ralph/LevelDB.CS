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
        private readonly Options options;
        private readonly DirectoryInfo databaseDir;
        private readonly TableCache tableCache;
        private readonly DbLock dbLock;
        private readonly VersionSet versions;

        private readonly AtomicBoolean shuttingDown = new AtomicBoolean();

        private readonly ReentrantLock mutex = new ReentrantLock();
        private readonly Condition backgroundCondition;

        private readonly IList<long> pendingOutputs = new List<long>(); // todo

        private ILogWriter log;

        private MemTable memTable;
        private MemTable immutableMemTable;

        private readonly InternalKeyComparator internalKeyComparator;

        private volatile Exception backgroundException;

        //private readonly ExecutorService compactionExecutor;
        private Task backgroundCompaction;

        private ManualCompaction manualCompaction;

        public DbImpl(Options options, DirectoryInfo databaseDir)
        {
            Preconditions.CheckNotNull(options, $"{nameof(options)} is null");
            Preconditions.CheckNotNull(databaseDir, $"{nameof(databaseDir)} is null");
            this.options = options;
            this.databaseDir = databaseDir;

            backgroundCondition = mutex.NewCondition();

            //use custom comparator if set
            IDBComparator comparator = options.Comparator();
            IUserComparator userComparator;
            if (comparator != null)
            {
                userComparator = new CustomUserComparator(comparator);
            }
            else
            {
                userComparator = new BytewiseComparator();
            }
            internalKeyComparator = new InternalKeyComparator(userComparator);
            memTable = new MemTable(internalKeyComparator);
            immutableMemTable = null;
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
            int tableCacheSize = options.MaxOpenFiles() - 10;
            tableCache = new TableCache(databaseDir, tableCacheSize, new InternalUserComparator(internalKeyComparator),
                options.VerifyChecksums());

            // create the version set

            // create the database dir if it does not already exist
            if (!databaseDir.Exists)
            {
                databaseDir.Create();
            }
            Preconditions.CheckArgument(Directory.Exists(databaseDir.FullName),
                $"Database directory '{databaseDir.FullName}' does not exist and could not be created");
            mutex.Lock();
            try
            {
                // lock the database dir
                dbLock = new DbLock(new FileInfo(Path.Combine(databaseDir.FullName, Filename.LockFileName())));

                // verify the "current" file
                FileInfo currentFile = new FileInfo(Path.Combine(databaseDir.FullName, Filename.CurrentFileName()));
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

                versions = new VersionSet(databaseDir, tableCache, internalKeyComparator);

                // load (and recover) current version
                versions.Recover();

                // Recover from all newer log files than the ones named in the
                // descriptor (new log files may have been added by the previous
                // incarnation without registering them in the descriptor).
                //
                // Note that PrevLogNumber() is no longer used, but we pay
                // attention to it in case we are recovering a database
                // produced by an older version of leveldb.
                long minLogNumber = versions.LogNumber;
                long previousLogNumber = versions.PrevLogNumber;
                IList<FileInfo> filenames = Filename.ListFiles(databaseDir);
                List<long> logs = (from filename in filenames
                        select Filename.ParseFileName(filename)
                        into fileInfo
                        where fileInfo != null && fileInfo.FileType == Filename.FileType.Log &&
                              (fileInfo.FileNumber >= minLogNumber || fileInfo.FileNumber == previousLogNumber)
                        select fileInfo.FileNumber)
                    .ToList();

                // Recover in the order in which the logs were generated
                VersionEdit edit = new VersionEdit();
                logs.Sort();
                foreach (long fileNumber in logs)
                {
                    long maxSequence = RecoverLogFile(fileNumber, edit);
                    if (versions.LastSequence < maxSequence)
                    {
                        versions.LastSequence = maxSequence;
                    }
                }

                // open transaction log
                long logFileNumber = versions.NextFileNumber;
                log = Logs.CreateLogWriter(
                    new FileInfo(Path.Combine(databaseDir.FullName, Filename.LogFileName(logFileNumber))),
                    logFileNumber);
                edit.LogNumber = log.FileNumber;

                // apply recovered edits
                versions.LogAndApply(edit);

                // cleanup unused files
                DeleteObsoleteFiles();

                // schedule compactions
                MaybeScheduleCompaction();
            }
            finally
            {
                mutex.Unlock();
            }
        }

        public void Dispose()
        {
            if (shuttingDown.GetAndSet(true))
            {
                return;
            }

            mutex.Lock();
            try
            {
                while (backgroundCompaction != null)
                {
                    backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                mutex.Unlock();
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
                versions.Destroy();
            }
            catch (IOException ignored)
            {
            }
            try
            {
                log.Close();
            }
            catch (IOException ignored)
            {
            }
            tableCache.Close();
            dbLock.Release();
        }

        public string GetProperty(string name)
        {
            CheckBackgroundException();
            return null;
        }

        private void DeleteObsoleteFiles()
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            // Make a set of all of the live files
            List<long> live = new List<long>(pendingOutputs);
            foreach (FileMetaData fileMetaData in versions.GetLiveFiles())
            {
                live.Add(fileMetaData.Number);
            }

            foreach (FileInfo file in Filename.ListFiles(databaseDir))
            {
                var fileInfo = Filename.ParseFileName(file);
                if (fileInfo == null)
                {
                    continue;
                }
                long number = fileInfo.FileNumber;
                bool keep = true;
                switch (fileInfo.FileType)
                {
                    case Filename.FileType.Log:
                        keep = ((number >= versions.LogNumber) ||
                                (number == versions.PrevLogNumber));
                        break;
                    case Filename.FileType.Descriptor:
                        // Keep my manifest file, and any newer incarnations'
                        // (in case there is a race that allows other incarnations)
                        keep = (number >= versions.ManifestFileNumber);
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
                        tableCache.Evict(number);
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
            mutex.Lock();
            try
            {
                // force compaction
                MakeRoomForWrite(true);

                // todo bg_error code
                while (immutableMemTable != null)
                {
                    backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                mutex.Unlock();
            }
        }

        public void CompactRange(int level, Slice start, Slice end)
        {
            Preconditions.CheckArgument(level >= 0, "level is negative");
            Preconditions.CheckArgument(level + 1 < DbConstants.NumLevels,
                $"level is greater than or equal to {DbConstants.NumLevels}");
            Preconditions.CheckNotNull(start, "start is null");
            Preconditions.CheckNotNull(end, "end is null");

            mutex.Lock();
            try
            {
                while (this.manualCompaction != null)
                {
                    backgroundCondition.AwaitUninterruptibly();
                }
                ManualCompaction manualCompaction = new ManualCompaction(level, start, end);
                this.manualCompaction = manualCompaction;

                MaybeScheduleCompaction();

                while (this.manualCompaction == manualCompaction)
                {
                    backgroundCondition.AwaitUninterruptibly();
                }
            }
            finally
            {
                mutex.Unlock();
            }
        }

        private void MaybeScheduleCompaction()
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            if (backgroundCompaction != null)
            {
                // Already scheduled
            }
            else if (shuttingDown.Value)
            {
                // DB is being shutdown; no more background compactions
            }
            else if (immutableMemTable == null &&
                     manualCompaction == null &&
                     !versions.NeedsCompaction())
            {
                // No work to be done
            }
            else
            {
                backgroundCompaction = Task.Factory.StartNew(() =>
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
                        backgroundException = e;
                    }
                });
            }
        }

        public void CheckBackgroundException()
        {
            var e = backgroundException;
            if (e != null)
            {
                throw new BackgroundProcessingException(e);
            }
        }

        private void BackgroundCall()
        {
            mutex.Lock();
            try
            {
                if (backgroundCompaction == null)
                {
                    return;
                }

                try
                {
                    if (!shuttingDown.Value)
                    {
                        BackgroundCompaction();
                    }
                }
                finally
                {
                    backgroundCompaction = null;
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
                        backgroundCondition.SignalAll();
                    }
                    finally
                    {
                        mutex.Unlock();
                    }
                }
            }
        }

        private void BackgroundCompaction()
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            CompactMemTableInternal();

            Compaction compaction;
            if (manualCompaction != null)
            {
                compaction = versions.CompactRange(manualCompaction.Level,
                    new InternalKey(manualCompaction.Begin, MaxSequenceNumber, Value),
                    new InternalKey(manualCompaction.End, 0, Deletion));
            }
            else
            {
                compaction = versions.PickCompaction();
            }

            if (compaction == null)
            {
                // no compaction
            }
            else if (manualCompaction == null && compaction.IsTrivialMove())
            {
                // Move file to next level
                Preconditions.CheckState(compaction.LevelInputs.Count == 1);
                FileMetaData fileMetaData = compaction.LevelInputs[0];
                compaction.Edit.DeleteFile(compaction.Level, fileMetaData.Number);
                compaction.Edit.AddFile(compaction.Level + 1, fileMetaData);
                versions.LogAndApply(compaction.Edit);
                // log
            }
            else
            {
                CompactionState compactionState = new CompactionState(compaction);
                DoCompactionWork(compactionState);
                CleanupCompaction(compactionState);
            }

            // manual compaction complete
            if (manualCompaction != null)
            {
                manualCompaction = null;
            }
        }

        private void CleanupCompaction(CompactionState compactionState)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            if (compactionState.builder != null)
            {
                compactionState.builder.Abandon();
            }
            else
            {
                Preconditions.CheckArgument(compactionState.outfile == null);
            }

            foreach (FileMetaData output in compactionState.outputs)
            {
                pendingOutputs.Remove(output.Number);
            }
        }

        private long RecoverLogFile(long fileNumber, VersionEdit edit)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());
            FileInfo file = new FileInfo(Path.Combine(databaseDir.FullName, Filename.LogFileName(fileNumber)));
            using (var stream = file.Open(FileMode.OpenOrCreate))
            {
                LogMonitor logMonitor = LogMonitors.LogMonitor();
                LogReader logReader = new LogReader(stream, logMonitor, true, 0);

                // Log(options_.info_log, "Recovering log #%llu", (unsigned long long) log_number);

                // Read all the records and add to a memtable
                long maxSequence = 0;
                MemTable memTable = null;
                for (Slice record = logReader.ReadRecord(); record != null; record = logReader.ReadRecord())
                {
                    SliceInput sliceInput = record.Input();
                    // read header
                    if (sliceInput.Available < 12)
                    {
                        logMonitor.Corruption(sliceInput.Available, "log record too small");
                        continue;
                    }
                    long sequenceBegin = sliceInput.ReadLong();
                    int updateSize = sliceInput.ReadInt();

                    // read entries
                    WriteBatchImpl writeBatch = ReadWriteBatch(sliceInput, updateSize);

                    // apply entries to memTable
                    if (memTable == null)
                    {
                        memTable = new MemTable(internalKeyComparator);
                    }
                    writeBatch.ForEach(new InsertIntoHandler(memTable, sequenceBegin));

                    // update the maxSequence
                    long lastSequence = sequenceBegin + updateSize - 1;
                    if (lastSequence > maxSequence)
                    {
                        maxSequence = lastSequence;
                    }

                    // flush mem table if necessary
                    if (memTable.ApproximateMemoryUsage > options.WriteBufferSize())
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
            mutex.Lock();
            LookupResult lookupResult;
            try
            {
                SnapshotImpl snapshot = GetSnapshot(options);
                lookupKey = new LookupKey(WrappedBuffer(key), snapshot.GetLastSequence());

                // First look in the memtable, then in the immutable memtable (if any).
                lookupResult = memTable.Get(lookupKey);
                if (lookupResult != null)
                {
                    Slice value = lookupResult.Value;
                    return value?.GetBytes();
                }
                if (immutableMemTable != null)
                {
                    lookupResult = immutableMemTable.Get(lookupKey);
                    if (lookupResult != null)
                    {
                        Slice value = lookupResult.Value;
                        return value?.GetBytes();
                    }
                }
            }
            finally
            {
                mutex.Unlock();
            }

            // Not in memTables; try live files in level order
            lookupResult = versions.Get(lookupKey);

            // schedule compaction if necessary
            mutex.Lock();
            try
            {
                if (versions.NeedsCompaction())
                {
                    MaybeScheduleCompaction();
                }
            }
            finally
            {
                mutex.Unlock();
            }

            if (lookupResult != null)
            {
                Slice value = lookupResult.Value;
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
            mutex.Lock();
            try
            {
                long sequenceEnd;
                if (updates.Size != 0)
                {
                    MakeRoomForWrite(false);

                    // Get sequence numbers for this change set
                    long sequenceBegin = versions.LastSequence + 1;
                    sequenceEnd = sequenceBegin + updates.Size - 1;

                    // Reserve this sequence in the version set
                    versions.LastSequence = sequenceEnd;

                    // Log write
                    Slice record = WriteWriteBatch(updates, sequenceBegin);
                    log.AddRecord(record, options.Sync());

                    // Update memtable
                    updates.ForEach(new InsertIntoHandler(memTable, sequenceBegin));
                }
                else
                {
                    sequenceEnd = versions.LastSequence;
                }

                return options.Snapshot() ? new SnapshotImpl(versions.Current, sequenceEnd) : null;
            }
            finally
            {
                mutex.Unlock();
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
            mutex.Lock();
            try
            {
                var rawIterator = InternalIterator();

                // filter any entries not visible in our snapshot
                SnapshotImpl snapshot = GetSnapshot(options);
                SnapshotSeekingIterator snapshotIterator =
                    new SnapshotSeekingIterator(rawIterator, snapshot, internalKeyComparator.UserComparator);
                return new SeekingIteratorAdapter(snapshotIterator);
            }
            finally
            {
                mutex.Unlock();
            }
        }

        ISeekingIterable<InternalKey, Slice> InternalIterable()
        {
            return new InternalSeekingIterable(this);
        }

        DbIterator InternalIterator()
        {
            mutex.Lock();
            try
            {
                // merge together the memTable, immutableMemTable, and tables in version set
                MemTable.MemTableIterator iterator = null;
                if (immutableMemTable != null)
                {
                    iterator = immutableMemTable.GetMemTableIterator();
                }
                Version current = versions.Current;
                return new DbIterator(memTable.GetMemTableIterator(), iterator, current.GetLevel0Files(),
                    current.GetLevelIterators(), internalKeyComparator);
            }
            finally
            {
                mutex.Unlock();
            }
        }

        public ISnapshot GetSnapshot()
        {
            CheckBackgroundException();
            mutex.Lock();
            try
            {
                return new SnapshotImpl(versions.Current, versions.LastSequence);
            }
            finally
            {
                mutex.Unlock();
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
                snapshot = new SnapshotImpl(versions.Current, versions.LastSequence);
                snapshot.Dispose(); // To avoid holding the snapshot active..
            }
            return snapshot;
        }

        private void MakeRoomForWrite(bool force)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            bool allowDelay = !force;

            while (true)
            {
                // todo background processing system need work
//            if (!bg_error_.ok()) {
//              // Yield previous error
//              s = bg_error_;
//              break;
//            } else
                if (allowDelay && versions.NumberOfFilesInLevel(0) > DbConstants.L0SlowdownWritesTrigger)
                {
                    // We are getting close to hitting a hard limit on the number of
                    // L0 files.  Rather than delaying a single write by several
                    // seconds when we hit the hard limit, start delaying each
                    // individual write by 1ms to reduce latency variance.  Also,
                    // this delay hands over some CPU to the compaction thread in
                    // case it is sharing the same core as the writer.
                    try
                    {
                        mutex.Unlock();
                        //Thread.sleep(1);
                    }
                    catch (Exception e)
                    {
                        //    Thread.currentThread().interrupt();
                        //    throw new RuntimeException(e);
                    }
                    finally
                    {
                        mutex.Lock();
                    }

                    // Do not delay a single write more than once
                    allowDelay = false;
                }
                else if (!force && memTable.ApproximateMemoryUsage <= options.WriteBufferSize())
                {
                    // There is room in current memtable
                    break;
                }
                else if (immutableMemTable != null)
                {
                    // We have filled up the current memtable, but the previous
                    // one is still being compacted, so we wait.
                    backgroundCondition.AwaitUninterruptibly();
                }
                else if (versions.NumberOfFilesInLevel(0) >= DbConstants.L0StopWritesTrigger)
                {
                    // There are too many level-0 files.
//                Log(options_.info_log, "waiting...\n");
                    backgroundCondition.AwaitUninterruptibly();
                }
                else
                {
                    // Attempt to switch to a new memtable and trigger compaction of old
                    Preconditions.CheckState(versions.PrevLogNumber == 0);

                    // close the existing log
                    try
                    {
                        log.Close();
                    }
                    catch (IOException e)
                    {
                        throw new Exception($"Unable to close log file {log.File}", e);
                    }

                    // open a new log
                    long logNumber = versions.NextFileNumber;
                    try
                    {
                        log = Logs.CreateLogWriter(
                            new FileInfo(Path.Combine(databaseDir.FullName, Filename.LogFileName(logNumber))),
                            logNumber);
                    }
                    catch (IOException e)
                    {
                        throw new Exception("Unable to open new log file " +
                                            new FileInfo(Path.Combine(databaseDir.FullName,
                                                Filename.LogFileName(logNumber))).FullName, e);
                    }

                    // create a new mem table
                    immutableMemTable = memTable;
                    memTable = new MemTable(internalKeyComparator);

                    // Do not force another compaction there is space available
                    force = false;

                    MaybeScheduleCompaction();
                }
            }
        }

        public void CompactMemTable()
        {
            mutex.Lock();
            try
            {
                CompactMemTableInternal();
            }
            finally
            {
                mutex.Unlock();
            }
        }

        private void CompactMemTableInternal()
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());
            if (immutableMemTable == null)
            {
                return;
            }

            try
            {
                // Save the contents of the memtable as a new Table
                VersionEdit edit = new VersionEdit();
                Version baseVersion = versions.Current;
                WriteLevel0Table(immutableMemTable, edit, baseVersion);

                if (shuttingDown.Value)
                {
                    throw new Exception("Database shutdown during memtable compaction");
                }

                // Replace immutable memtable with the generated Table
                edit.PreviousLogNumber = 0;
                edit.LogNumber = (log.FileNumber); // Earlier logs no longer needed
                versions.LogAndApply(edit);

                immutableMemTable = null;

                DeleteObsoleteFiles();
            }
            finally
            {
                backgroundCondition.SignalAll();
            }
        }


        private void WriteLevel0Table(MemTable mem, VersionEdit edit, Version baseVersion)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            // skip empty mem table
            if (mem.IsEmpty)
            {
                return;
            }

            // write the memtable to a new sstable
            long fileNumber = versions.NextFileNumber;
            pendingOutputs.Add(fileNumber);
            mutex.Unlock();
            FileMetaData meta;
            try
            {
                meta = BuildTable(mem, fileNumber);
            }
            finally
            {
                mutex.Lock();
            }
            pendingOutputs.Remove(fileNumber);

            // Note that if file size is zero, the file has been deleted and
            // should not be added to the manifest.
            int level = 0;
            if (meta != null && meta.FileSize > 0)
            {
                Slice minUserKey = meta.Smallest.UserKey;
                Slice maxUserKey = meta.Largest.UserKey;
                if (baseVersion != null)
                {
                    level = baseVersion.PickLevelForMemTableOutput(minUserKey, maxUserKey);
                }
                edit.AddFile(level, meta);
            }
        }

        private FileMetaData BuildTable(MemTable data, long fileNumber)
        {
            var file = new FileInfo(Path.Combine(databaseDir.FullName, Filename.TableFileName(fileNumber)));
            try
            {
                InternalKey smallest = null;
                InternalKey largest = null;
                var channel = file.Open(FileMode.OpenOrCreate, FileAccess.ReadWrite);
                try
                {
                    TableBuilder tableBuilder =
                        new TableBuilder(options, channel, new InternalUserComparator(internalKeyComparator));
                    var memTableIterator = data.GetMemTableIterator();
                    for (Entry<InternalKey, Slice> entry = memTableIterator.Next(); memTableIterator.HasNext(); entry = memTableIterator.Next())
                    {
                        // update keys
                        InternalKey key = entry.Key;
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
                FileMetaData fileMetaData = new FileMetaData(fileNumber, file.Length, smallest, largest);

                // verify table can be opened
                tableCache.NewIterator(fileMetaData);

                pendingOutputs.Remove(fileNumber);

                return fileMetaData;
            }
            catch (IOException e)
            {
                file.Delete();
                throw e;
            }
        }

        private void DoCompactionWork(CompactionState compactionState)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());
            Preconditions.CheckArgument(versions.NumberOfBytesInLevel(compactionState.Compaction.Level) > 0);
            Preconditions.CheckArgument(compactionState.builder == null);
            Preconditions.CheckArgument(compactionState.outfile == null);

            // todo track snapshots
            compactionState.smallestSnapshot = versions.LastSequence;

            // Release mutex while we're actually doing the compaction work
            mutex.Unlock();
            try
            {
                MergingIterator iterator = versions.MakeInputIterator(compactionState.Compaction);

                Slice currentUserKey = null;
                bool hasCurrentUserKey = false;

                long lastSequenceForKey = MaxSequenceNumber;
                while (iterator.HasNext() && !shuttingDown.Value)
                {
                    // always give priority to compacting the current mem table
                    mutex.Lock();
                    try
                    {
                        CompactMemTableInternal();
                    }
                    finally
                    {
                        mutex.Unlock();
                    }

                    InternalKey key = iterator.Peek().Key;
                    if (compactionState.Compaction.ShouldStopBefore(key) && compactionState.builder != null)
                    {
                        FinishCompactionOutputFile(compactionState);
                    }

                    // Handle key/value, add to state, etc.
                    bool drop = false;
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
                        if (!hasCurrentUserKey || internalKeyComparator.UserComparator
                                .Compare(key.UserKey, currentUserKey) != 0)
                        {
                            // First occurrence of this user key
                            currentUserKey = key.UserKey;
                            hasCurrentUserKey = true;
                            lastSequenceForKey = MaxSequenceNumber;
                        }

                        if (lastSequenceForKey <= compactionState.smallestSnapshot)
                        {
                            // Hidden by an newer entry for same user key
                            drop = true; // (A)
                        }
                        else if (key.ValueType == Deletion &&
                                 key.SequenceNumber <= compactionState.smallestSnapshot &&
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
                        if (compactionState.builder == null)
                        {
                            OpenCompactionOutputFile(compactionState);
                        }
                        if (compactionState.builder.GetEntryCount() == 0)
                        {
                            compactionState.currentSmallest = key;
                        }
                        compactionState.currentLargest = key;
                        compactionState.builder.Add(key.Encode(), iterator.Peek().Value);

                        // Close output file if it is big enough
                        if (compactionState.builder.GetFileSize() >=
                            compactionState.Compaction.MaxOutputFileSize)
                        {
                            FinishCompactionOutputFile(compactionState);
                        }
                    }
                    iterator.Next();
                }

                if (shuttingDown.Value)
                {
                    throw new Exception("DB shutdown during compaction");
                }
                if (compactionState.builder != null)
                {
                    FinishCompactionOutputFile(compactionState);
                }
            }
            finally
            {
                mutex.Lock();
            }

            // todo port CompactionStats code

            installCompactionResults(compactionState);
        }

        private void OpenCompactionOutputFile(CompactionState compactionState)
        {
            Preconditions.CheckNotNull(compactionState, $"{nameof(compactionState)} is null");
            Preconditions.CheckArgument(compactionState.builder == null,
                $"{nameof(compactionState)} builder is not null");

            mutex.Lock();
            try
            {
                long fileNumber = versions.NextFileNumber;
                pendingOutputs.Add(fileNumber);
                compactionState.currentFileNumber = fileNumber;
                compactionState.currentFileSize = 0;
                compactionState.currentSmallest = null;
                compactionState.currentLargest = null;

                FileInfo file = new FileInfo(Path.Combine(databaseDir.FullName, Filename.TableFileName(fileNumber)));
                compactionState.outfile = file.Open(FileMode.Open);
                compactionState.builder = new TableBuilder(options, compactionState.outfile,
                    new InternalUserComparator(internalKeyComparator));
            }
            finally
            {
                mutex.Unlock();
            }
        }

        private void FinishCompactionOutputFile(CompactionState compactionState)
        {
            Preconditions.CheckNotNull(compactionState, $"{nameof(compactionState)} is null");
            Preconditions.CheckArgument(compactionState.outfile != null);
            Preconditions.CheckArgument(compactionState.builder != null);

            long outputNumber = compactionState.currentFileNumber;
            Preconditions.CheckArgument(outputNumber != 0);

            long currentEntries = compactionState.builder.GetEntryCount();
            compactionState.builder.Finish();

            long currentBytes = compactionState.builder.GetFileSize();
            compactionState.currentFileSize = currentBytes;
            compactionState.totalBytes += currentBytes;

            FileMetaData currentFileMetaData = new FileMetaData(compactionState.currentFileNumber,
                compactionState.currentFileSize,
                compactionState.currentSmallest,
                compactionState.currentLargest);
            compactionState.outputs.Add(currentFileMetaData);

            compactionState.builder = null;

            //compactionState.outfile.Force(true);
            compactionState.outfile.Dispose();
            compactionState.outfile = null;

            if (currentEntries > 0)
            {
                // Verify that the table is usable
                tableCache.NewIterator(outputNumber);
            }
        }

        private void installCompactionResults(CompactionState compact)
        {
            Preconditions.CheckState(mutex.IsHeldByCurrentThread());

            // Add compaction outputs
            compact.Compaction.AddInputDeletions(compact.Compaction.Edit);
            int level = compact.Compaction.Level;
            foreach (FileMetaData output in compact.outputs)
            {
                compact.Compaction.Edit.AddFile(level + 1, output);
                pendingOutputs.Remove(output.Number);
            }

            try
            {
                versions.LogAndApply(compact.Compaction.Edit);
                DeleteObsoleteFiles();
            }
            catch (IOException e)
            {
                // Compaction failed for some reason.  Simply discard the work and try again later.

                // Discard any files we may have created during this failed compaction
                foreach (FileMetaData output in compact.outputs)
                {
                    FileInfo file = new FileInfo(
                        Path.Combine(databaseDir.FullName, Filename.TableFileName(output.Number)));
                    file.Delete();
                }
                compact.outputs.Clear();
            }
        }

        int NumberOfFilesInLevel(int level)
        {
            return versions.Current.NumberOfFilesInLevel(level);
        }

        public long[] GetApproximateSizes(params Range[] ranges)
        {
            Preconditions.CheckNotNull(ranges, $"{nameof(ranges)} is null");
            long[] sizes = new long[ranges.Length];
            for (int i = 0; i < ranges.Length; i++)
            {
                Range range = ranges[i];
                sizes[i] = GetApproximateSizes(range);
            }
            return sizes;
        }

        public long GetApproximateSizes(Range range)
        {
            Version v = versions.Current;

            InternalKey startKey = new InternalKey(WrappedBuffer(range.Start()), MaxSequenceNumber, Value);
            InternalKey limitKey = new InternalKey(WrappedBuffer(range.Limit()), MaxSequenceNumber, Value);
            long startOffset = v.GetApproximateOffsetOf(startKey);
            long limitOffset = v.GetApproximateOffsetOf(limitKey);

            return (limitOffset >= startOffset ? limitOffset - startOffset : 0);
        }

        public long GetMaxNextLevelOverlappingBytes()
        {
            return versions.GetMaxNextLevelOverlappingBytes();
        }

        private WriteBatchImpl ReadWriteBatch(SliceInput record, int updateSize)
        {
            WriteBatchImpl writeBatch = new WriteBatchImpl();
            int entries = 0;
            while (record.CanRead)
            {
                entries++;
                ValueType valueType = GetValueTypeByPersistentId(record.ReadByteAlt());
                if (valueType == Value)
                {
                    Slice key = ReadLengthPrefixedBytes(record);
                    Slice value = ReadLengthPrefixedBytes(record);
                    writeBatch.Put(key, value);
                }
                else if (valueType == Deletion)
                {
                    Slice key = ReadLengthPrefixedBytes(record);
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

        private readonly object suspensionMutex = new object();
        private int suspensionCounter;

        public void SuspendCompactions()
        {
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;
            Task.Factory.StartNew(() =>
            {
                try
                {
                    lock (suspensionMutex)
                    {
                        suspensionCounter++;
                        Monitor.PulseAll(suspensionMutex);
                        while (suspensionCounter > 0 && !token.IsCancellationRequested)
                        {
                            Monitor.Wait(suspensionMutex, 500);
                        }
                    }
                }
                catch (Exception)
                {
                    // ignored
                }
            }, token);

            lock (suspensionMutex)
            {
                while (suspensionCounter < 1)
                {
                    Monitor.Wait(suspensionMutex);
                }
            }
        }

        public void ResumeCompactions()
        {
            lock (suspensionMutex)
            {
                suspensionCounter--;
                Monitor.PulseAll(suspensionMutex);
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

            internal readonly IList<FileMetaData> outputs = new List<FileMetaData>();

            internal long smallestSnapshot;

            // State kept for output being generated
            internal FileStream outfile;

            internal TableBuilder builder;

            // Current file being generated
            internal long currentFileNumber;

            internal long currentFileSize;
            internal InternalKey currentSmallest;
            internal InternalKey currentLargest;

            internal long totalBytes;

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