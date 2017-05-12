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
using System.Linq;
using LevelDB.InnerUtil;
using LevelDB.Util;
using Xunit;
using static LevelDB.InnerUtil.ListHelper;

namespace LevelDB.Table
{
    public class BlockTest
    {
        [Fact]
        public void TestEmptyBuffer()
        {
            Assert.Throws<ArgumentException>(() => new Block(Slices.EmptySlice, new BytewiseComparator()));
        }

        [Fact]
        public void TestEmptyBlock()
        {
            BlockTestInternal(int.MaxValue);
        }

        [Fact]
        public void TestSingleEntry()
        {
            BlockTestInternal(int.MaxValue, BlockHelper.CreateBlockEntry("name", "dain sundstrom"));
        }

        [Fact]
        public void TestMultipleEntriesWithNonSharedKey()
        {
            BlockTestInternal(int.MaxValue,
                BlockHelper.CreateBlockEntry("beer", "Lagunitas IPA"),
                BlockHelper.CreateBlockEntry("scotch", "Highland Park"));
        }

        [Fact]
        public void TestMultipleEntriesWithSharedKey()
        {
            BlockTestInternal(int.MaxValue,
                BlockHelper.CreateBlockEntry("beer/ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.CreateBlockEntry("beer/ipa", "Lagunitas IPA"),
                BlockHelper.CreateBlockEntry("scotch", "Highland Park"));
        }

        [Fact]
        public void TestMultipleEntriesWithNonSharedKeyAndRestartPositions()
        {
            var entries = new[]
            {
                BlockHelper.CreateBlockEntry("ale", "Lagunitas  Little Sumpin’ Sumpin’"),
                BlockHelper.CreateBlockEntry("ipa", "Lagunitas IPA"),
                BlockHelper.CreateBlockEntry("stout", "Lagunitas Imperial Stout"),
                BlockHelper.CreateBlockEntry("strong", "Lagavulin")
            };

            for (var i = 1; i < entries.Length; i++)
            {
                BlockTestInternal(i, entries);
            }
        }

        [Fact]
        public void TestMultipleEntriesWithSharedKeyAndRestartPositions()
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
                BlockTestInternal(i, entries);
            }
        }

        private static void BlockTestInternal(int blockRestartInterval, params BlockEntry[] entries)
        {
            BlockTestInternal(blockRestartInterval, entries.ToList());
        }

        private static void BlockTestInternal(int blockRestartInterval, List<BlockEntry> entries)
        {
            var builder = new BlockBuilder(256, blockRestartInterval, new BytewiseComparator());
            foreach (var entry in entries)
            {
                builder.Add(entry);
            }
            Assert.Equal(BlockHelper.EstimateBlockSize(blockRestartInterval, entries), builder.CurrentSizeEstimate());
            var blockSlice = builder.Finish();
            Assert.Equal(BlockHelper.EstimateBlockSize(blockRestartInterval, entries), builder.CurrentSizeEstimate());
            var block = new Block(blockSlice, new BytewiseComparator());
            Assert.Equal(BlockHelper.EstimateBlockSize(blockRestartInterval, entries), block.Size);
            var blockIterator = block.GetBlockIterator();
            BlockHelper.AssertSequence(blockIterator, entries);
            blockIterator.SeekToFirst();
            BlockHelper.AssertSequence(blockIterator, entries);
            foreach (var entry in entries)
            {
                var nextEntries = Sublist(entries, entries.IndexOf(entry), entries.Count);
                blockIterator.Seek(entry.Key);
                BlockHelper.AssertSequence(blockIterator, nextEntries);

                blockIterator.Seek(BlockHelper.Before(entry));
                BlockHelper.AssertSequence(blockIterator, nextEntries);

                blockIterator.Seek(BlockHelper.After(entry));
                BlockHelper.AssertSequence(blockIterator, Sublist(nextEntries, 1, nextEntries.Count));
            }
            blockIterator.Seek(Slices.WrappedBuffer(new byte[]
            {
                0xFF, 0xFF, 0xFF, 0xFF
            }));
            BlockHelper.AssertSequence(blockIterator, new List<Entry<Slice, Slice>>());
        }
    }
}