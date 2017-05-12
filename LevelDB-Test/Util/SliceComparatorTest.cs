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

using System.Text;
using Xunit;
using static LevelDB.Util.SliceComparer;

namespace LevelDB.Util
{
    public class SliceComparatorTest
    {
        [Fact]
        public void TestSliceComparison()
        {
            Assert.True(SliceComparator.Compare(
                            Slices.CopiedBuffer("beer/ipa", Encoding.UTF8),
                            Slices.CopiedBuffer("beer/ale", Encoding.UTF8))
                        > 0);

            Assert.True((SliceComparator.Compare(
                             Slices.WrappedBuffer(new[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                             Slices.WrappedBuffer(new[] {(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}))
                         > 0));

            Assert.True((SliceComparator.Compare(
                             Slices.WrappedBuffer(new[] {(byte) 0xFF}),
                             Slices.WrappedBuffer(new[] {(byte) 0x00}))
                         > 0));

            AssertAllEqual(Slices.CopiedBuffer("abcdefghijklmnopqrstuvwxyz", Encoding.UTF8),
                Slices.CopiedBuffer("abcdefghijklmnopqrstuvwxyz", Encoding.UTF8));
        }

        public static void AssertAllEqual(Slice left, Slice right)
        {
            for (var i = 0; i < left.Length; i++)
            {
                Assert.Equal(SliceComparator.Compare(left.Sliced(0, i), right.Sliced(0, i)), 0);
                Assert.Equal(SliceComparator.Compare(right.Sliced(0, i), left.Sliced(0, i)), 0);
            }
            // differ in last byte only
            for (var i = 1; i < left.Length; i++)
            {
                var slice = right.Sliced(0, i);
                var lastReadableByte = slice.Length - 1;
                slice.SetByte(lastReadableByte, (byte) (slice.GetByte(lastReadableByte) + 1));
                Assert.True(SliceComparator.Compare(left.Sliced(0, i), slice) < 0);
                Assert.True(SliceComparator.Compare(slice, left.Sliced(0, i)) > 0);
            }
        }
    }
}