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
using System.Text;
using LevelDB.Impl;
using LevelDB.Table;
using LevelDB.Util;
using Xunit;

namespace LevelDB.InnerUtil
{
    public static class BlockHelper
    {
        public static int EstimateBlockSize(int blockRestartInterval, List<BlockEntry> entries)
        {
            if (entries.Count == 0)
            {
                return SizeOf.Int;
            }
            var restartCount = (int) Math.Ceiling(1.0 * entries.Count / blockRestartInterval);
            return EstimateEntriesSize(blockRestartInterval, entries) +
                   restartCount * SizeOf.Int +
                   SizeOf.Int;
        }

        public static void AssertSequence<TK, TV>(ISeekingIterator<TK, TV> actuals, params Entry<TK, TV>[] expecteds)
        {
            AssertSequence(actuals, expecteds.ToList());
        }

        public static void AssertSequence<TK, TV>(ISeekingIterator<TK, TV> actuals,
            IEnumerable<Entry<TK, TV>> expecteds)
        {
            Assert.NotNull(actuals);

            foreach (var expected in expecteds)
            {
                Assert.True(actuals.HasNext());
                AssertEntryEquals(expected, actuals.Peek());
                AssertEntryEquals(expected, actuals.Next());
            }
            Assert.False(actuals.HasNext());

            try
            {
                actuals.Peek();
                Assert.True(false, "expected Exception");
            }
            catch (Exception)
            {
                // ignored
            }
            try
            {
                actuals.Next();
                Assert.True(false, "expected Exception");
            }
            catch (Exception)
            {
                // ignored
            }
        }

        public static void AssertEntryEquals<TK, TV>(Entry<TK, TV> expected, Entry<TK, TV> actual)
        {
            if (actual.Key is Slice)
            {
                AssertSliceEquals(expected.Key as Slice, actual.Key as Slice);
                AssertSliceEquals(expected.Value as Slice, actual.Value as Slice);
            }
            Assert.Equal(actual, expected);
        }

        public static void AssertSliceEquals(Slice expected, Slice actual)
        {
            Assert.Equal(expected.ToString(Encoding.UTF8), actual.ToString(Encoding.UTF8));
        }

        public static string BeforeString<T>(Entry<string, T> expectedEntry)
        {
            var key = expectedEntry.Key;
            var lastByte = key[key.Length - 1];
            return key.Substring(0, key.Length - 1) + (char) (lastByte - 1);
        }

        public static string AfterString<T>(Entry<string, T> expectedEntry)
        {
            var key = expectedEntry.Key;
            var lastByte = key[key.Length - 1];
            return key.Substring(0, key.Length - 1) + (char) (lastByte + 1);
        }

        public static Slice Before<T>(Entry<Slice, T> expectedEntry)
        {
            var slice = expectedEntry.Key.CopySlice(0, expectedEntry.Key.Length);
            var lastByte = slice.Length - 1;
            slice.SetByte(lastByte, (byte) (slice.GetByte(lastByte) - 1));
            return slice;
        }

        public static Slice After<T>(Entry<Slice, T> expectedEntry)
        {
            var slice = expectedEntry.Key.CopySlice(0, expectedEntry.Key.Length);
            var lastByte = slice.Length - 1;
            slice.SetByte(lastByte, (byte) (slice.GetByte(lastByte) + 1));
            return slice;
        }

        public static int EstimateEntriesSize(int blockRestartInterval, List<BlockEntry> entries)
        {
            var size = 0;
            Slice previousKey = null;
            var restartBlockCount = 0;
            foreach (var entry in entries)
            {
                int nonSharedBytes;
                if (restartBlockCount < blockRestartInterval)
                {
                    nonSharedBytes = entry.Key.Length - BlockBuilder.CalculateSharedBytes(entry.Key, previousKey);
                }
                else
                {
                    nonSharedBytes = entry.Key.Length;
                    restartBlockCount = 0;
                }
                size += nonSharedBytes +
                        entry.Value.Length +
                        SizeOf.Byte * 3; // 3 bytes for sizes

                previousKey = entry.Key;
                restartBlockCount++;
            }
            return size;
        }

        public static BlockEntry CreateBlockEntry(string key, string val)
        {
            var keySlice = Slices.CopiedBuffer(key, Encoding.UTF8);
            var valueSlice = Slices.CopiedBuffer(val, Encoding.UTF8);
            return new BlockEntry(keySlice, valueSlice);
        }
    }
}