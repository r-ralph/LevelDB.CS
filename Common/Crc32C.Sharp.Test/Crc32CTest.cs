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
using System.Text;
using Xunit;
using static Crc32C.Sharp.Crc32C;

namespace Crc32C.Sharp.Test
{
    public class Crc32CTest
    {
        [Theory]
        [MemberData(nameof(ProducesDifferentCrcsDataSource.TestData), MemberType =
            typeof(ProducesDifferentCrcsDataSource))]
        public void TestCrc(uint expectedCrc, byte[] data)
        {
            Assert.Equal(expectedCrc, ComputeCrc(data));
        }

        private static class ProducesDifferentCrcsDataSource
        {
            private static readonly List<object[]> Data = new List<object[]>
            {
                new object[] {0x8a9136aa, ArrayOf(32, 0)},
                new object[] {0x62a8ab43, ArrayOf(32, 0xff)},
                new object[] {0x46dd794e, ArrayOf(32, position => (byte) position)},
                new object[] {0x113fdb5c, ArrayOf(32, position => (byte) (31 - position))},
                new object[]
                {
                    0xd9963a56, ArrayOfByte(new[]
                    {
                        0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
                        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
                        0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
                    })
                }
            };

            public static IEnumerable<object[]> TestData => Data;
        }


        [Fact]
        public void TestProducesDifferentCrcs()
        {
            Assert.False(ComputeCrc(Encoding.ASCII.GetBytes("a")) == ComputeCrc(Encoding.ASCII.GetBytes("foo")));
        }

        [Fact]
        public void TestComposes()
        {
            var crc = new Crc32C();
            crc.Update(Encoding.ASCII.GetBytes("hello "), 0, 6);
            crc.Update(Encoding.ASCII.GetBytes("world"), 0, 5);

            Assert.Equal(crc.GetIntValue(), ComputeCrc(Encoding.ASCII.GetBytes("hello world")));
        }

        [Fact]
        public void TestMask()
        {
            var crc = new Crc32C();
            crc.Update(Encoding.ASCII.GetBytes("foo"), 0, 3);

            Assert.Equal(crc.GetMaskedValue(), Mask(crc.GetIntValue()));
            Assert.False(crc.GetIntValue() == crc.GetMaskedValue(), "crc should not match masked crc");
            Assert.False(crc.GetIntValue() == Mask(crc.GetMaskedValue()), "crc should not match double masked crc");
            Assert.Equal(crc.GetIntValue(), Unmask(crc.GetMaskedValue()));
            Assert.Equal(crc.GetIntValue(), Unmask(Unmask(Mask(crc.GetMaskedValue()))));
        }

        private static uint ComputeCrc(byte[] data)
        {
            var crc = new Crc32C();
            crc.Update(data, 0, data.Length);
            return crc.GetIntValue();
        }

        private static byte[] ArrayOf(int size, byte value)
        {
            var result = new byte[size];
            Fill(result, value);
            return result;
        }

        private static byte[] ArrayOf(int size, Func<int, byte> generator)
        {
            var result = new byte[size];
            for (var i = 0; i < result.Length; ++i)
            {
                result[i] = generator(i);
            }

            return result;
        }

        private static byte[] ArrayOfByte(IReadOnlyList<int> bytes)
        {
            var result = new byte[bytes.Count];
            for (var i = 0; i < result.Length; ++i)
            {
                result[i] = (byte) bytes[i];
            }

            return result;
        }

        public static void Fill<T>(T[] originalArray, T with)
        {
            for (var i = 0; i < originalArray.Length; i++)
            {
                originalArray[i] = with;
            }
        }
    }
}