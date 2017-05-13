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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using LevelDB.Guava;
using LevelDB.Impl;
using LevelDB.InnerUtil;
using LevelDB.Util;
using Xunit;
using static LevelDB.InnerUtil.ListHelper;

namespace LevelDB.Table
{
    public abstract class TableTest : IDisposable
    {
        private FileInfo _file;
        private FileStream _fileChannel;

        protected TableTest()
        {
            ReopenFile();
            Preconditions.CheckState(0 == _fileChannel.Position,
                $"Expected fileChannel.position {_fileChannel.Position} to be 0");
        }

        public void Dispose()
        {
            Disposables.DisposeQuietly(_fileChannel);
        }

        protected abstract Table CreateTable(string name, FileStream fileStream, IComparer<Slice> comparator,
            bool verifyChecksums);

        [Fact]
        public void TestEmptyFile()
        {
            Assert.Throws<ArgumentException>(
                () => CreateTable(_file.FullName, _fileChannel, new BytewiseComparator(), true));
        }

        [Fact]
        public void TestEmptyBlock()
        {
            TestTable(int.MaxValue, int.MaxValue);
        }

        [Fact]
        public void TestSingleEntrySingleBlock()
        {
            TestTable(int.MaxValue, int.MaxValue,
                BlockHelper.CreateBlockEntry("name", "dain sundstrom"));
        }

        [Fact]
        public void TestMultipleEntriesWithSingleBlock()
        {
            var entries = new[]
            {
                BlockHelper.CreateBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.CreateBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.CreateBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.CreateBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.CreateBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.CreateBlockEntry("scotch/strong", "Lagavulin")
            };

            for (var i = 1; i < entries.Length; i++)
            {
                TestTable(int.MaxValue, i, entries);
            }
        }

        [Fact]
        public void TestMultipleEntriesWithMultipleBlock()
        {
            var entries = new[]
            {
                BlockHelper.CreateBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.CreateBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.CreateBlockEntry("beer/stout", "Lagunitas Imperial Stout"),
                BlockHelper.CreateBlockEntry("scotch/light", "Oban 14"),
                BlockHelper.CreateBlockEntry("scotch/medium", "Highland Park"),
                BlockHelper.CreateBlockEntry("scotch/strong", "Lagavulin")
            };

            // one entry per block
            TestTable(1, int.MaxValue, entries);

            // about 3 blocks
            TestTable(BlockHelper.EstimateBlockSize(int.MaxValue, entries.ToList()) / 3, int.MaxValue, entries);
        }

        private void TestTable(int blockSize, int blockRestartInterval, params BlockEntry[] entries)
        {
            TestTable(blockSize, blockRestartInterval, entries.ToList());
        }

        private void TestTable(int blockSize, int blockRestartInterval, List<BlockEntry> entries)
        {
            ReopenFile();
            var options = new Options().BlockSize(blockSize).BlockRestartInterval(blockRestartInterval);

            var builder = new TableBuilder(options, _fileChannel, new BytewiseComparator());
            foreach (var entry in entries)
            {
                builder.Add(entry);
            }
            builder.Finish();

			_fileChannel.Position = 0;
            var table = CreateTable(_file.FullName, _fileChannel, new BytewiseComparator(), true);

            ISeekingIterator<Slice, Slice> seekingIterator = table.GetTableIterator();
            BlockHelper.AssertSequence(seekingIterator, entries);

            seekingIterator.SeekToFirst();
            BlockHelper.AssertSequence(seekingIterator, entries);

            long lastApproximateOffset = 0;
            foreach (var entry in entries)
            {
                var nextEntries = Sublist(entries, entries.IndexOf(entry), entries.Count);
                seekingIterator.Seek(entry.Key);
                BlockHelper.AssertSequence(seekingIterator, nextEntries);

                seekingIterator.Seek(BlockHelper.Before(entry));
                BlockHelper.AssertSequence(seekingIterator, nextEntries);

                seekingIterator.Seek(BlockHelper.After(entry));
                BlockHelper.AssertSequence(seekingIterator, Sublist(nextEntries, 1, nextEntries.Count));

                var approximateOffset = table.GetApproximateOffsetOf(entry.Key);
                Assert.True(approximateOffset >= lastApproximateOffset);
                lastApproximateOffset = approximateOffset;
            }

            var endKey = Slices.WrappedBuffer(new byte[] {0xFF, 0xFF, 0xFF, 0xFF});
            seekingIterator.Seek(endKey);
            BlockHelper.AssertSequence(seekingIterator, new List<Entry<Slice, Slice>>());

            var approximateOffset2 = table.GetApproximateOffsetOf(endKey);
            Assert.True(approximateOffset2 >= lastApproximateOffset);
        }

        private void ReopenFile()
        {
            _file = new FileInfo(Path.GetTempPath() + Guid.NewGuid() + ".csv");
            if (_file.Exists)
            {
                _file.Delete();
            }
            _fileChannel = _file.Create();
        }
    }
}