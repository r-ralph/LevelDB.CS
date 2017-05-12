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

using Xunit;

namespace LevelDB.Util
{
    public class VariableLengthQuantityTest
    {
        [Fact]
        public void TestWriteVariableLengthInt()
        {
            TestVariableLengthInt(0x0);
            TestVariableLengthInt(0xf);
            TestVariableLengthInt(0xff);
            TestVariableLengthInt(0xfff);
            TestVariableLengthInt(0xffff);
            TestVariableLengthInt(0xfffff);
            TestVariableLengthInt(0xffffff);
            TestVariableLengthInt(0xfffffff);
            TestVariableLengthInt(0xffffffff);
        }

        private static void TestVariableLengthInt(uint value)
        {
            var output = Slices.Allocate(5).Output();
            VariableLengthQuantity.WriteVariableLengthInt(value, output);
            Assert.Equal(output.Size(), VariableLengthQuantity.VariableLengthSize(value));
            var actual = VariableLengthQuantity.ReadVariableLengthInt(output.Sliced().Input());
            Assert.Equal(actual, value);
        }

        [Fact]
        public void TestWriteVariableLengthLong()
        {
            TestVariableLengthLong(0x0);
            TestVariableLengthLong(0xf);
            TestVariableLengthLong(0xff);
            TestVariableLengthLong(0xfff);
            TestVariableLengthLong(0xffff);
            TestVariableLengthLong(0xfffff);
            TestVariableLengthLong(0xffffff);
            TestVariableLengthLong(0xfffffff);
            TestVariableLengthLong(0xffffffff);
            TestVariableLengthLong(0xfffffffff);
            TestVariableLengthLong(0xffffffffff);
            TestVariableLengthLong(0xfffffffffff);
            TestVariableLengthLong(0xffffffffffff);
            TestVariableLengthLong(0xfffffffffffff);
            TestVariableLengthLong(0xffffffffffffff);
            TestVariableLengthLong(0xfffffffffffffff);
            TestVariableLengthLong(0xffffffffffffffff);
        }

        private static void TestVariableLengthLong(ulong value)
        {
            var output = Slices.Allocate(12).Output();
            VariableLengthQuantity.WriteVariableLengthLong(value, output);
            Assert.Equal(output.Size(), VariableLengthQuantity.VariableLengthSize(value));
            var actual = VariableLengthQuantity.ReadVariableLengthLong(output.Sliced().Input());
            Assert.Equal(actual, value);
        }
    }
}