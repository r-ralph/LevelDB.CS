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
using System.IO;

namespace LevelDB.Util
{
    public static class VariableLengthQuantity
    {
        public static int VariableLengthSize(uint value)
        {
            var size = 1;
            while ((value & ~0x7f) != 0)
            {
                value >>= 7;
                size++;
            }
            return size;
        }

        public static int VariableLengthSize(ulong value)
        {
            var size = 1;
            while ((value & unchecked((ulong) ~0x7f)) != 0)
            {
                value >>= 7;
                size++;
            }
            return size;
        }

        public static void WriteVariableLengthInt(uint value, SliceOutput sliceOutput)
        {
            const byte highBitMask = 0x80;
            if (value < 1 << 7)
            {
                sliceOutput.WriteByte((byte) value);
            }
            else if (value < 1 << 14)
            {
                sliceOutput.WriteByte((byte) (value | highBitMask));
                sliceOutput.WriteByte((byte) (value >> 7));
            }
            else if (value < 1 << 21)
            {
                sliceOutput.WriteByte((byte) (value | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 7) | highBitMask));
                sliceOutput.WriteByte((byte) (value >> 14));
            }
            else if (value < 1 << 28)
            {
                sliceOutput.WriteByte((byte) (value | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 7) | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 14) | highBitMask));
                sliceOutput.WriteByte((byte) (value >> 21));
            }
            else
            {
                sliceOutput.WriteByte((byte) (value | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 7) | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 14) | highBitMask));
                sliceOutput.WriteByte((byte) ((value >> 21) | highBitMask));
                sliceOutput.WriteByte((byte) (value >> 28));
            }
        }

        public static void WriteVariableLengthLong(ulong value, SliceOutput sliceOutput)
        {
            // while value more than the first 7 bits set
            while ((value & unchecked((ulong) ~0x7f)) != 0)
            {
                sliceOutput.WriteByte((byte) ((value & 0x7f) | 0x80));
                value >>= 7;
            }
            sliceOutput.WriteByte((byte) value);
        }

        public static uint ReadVariableLengthInt(SliceInput sliceInput)
        {
            uint result = 0;
            for (var shift = 0; shift <= 28; shift += 7)
            {
                uint b = sliceInput.ReadByteAlt();
                // add the lower 7 bits to the result
                result |= (b & 0x7f) << shift;

                // if high bit is not set, this is the last byte in the number
                if ((b & 0x80) == 0)
                {
                    return result;
                }
            }
            throw new FormatException("last byte of variable length int has high bit set");
        }

        public static uint ReadVariableLengthInt(MemoryStream sliceInput)
        {
            uint result = 0;
            for (var shift = 0; shift <= 28; shift += 7)
            {
                var b = (uint) sliceInput.ReadByte();
                // add the lower 7 bits to the result
                result |= (b & 0x7f) << shift;
                // if high bit is not set, this is the last byte in the number
                if ((b & 0x80) == 0)
                {
                    return result;
                }
            }
            throw new FormatException("last byte of variable length int has high bit set");
        }

        public static ulong ReadVariableLengthLong(SliceInput sliceInput)
        {
            ulong result = 0;
            for (var shift = 0; shift <= 63; shift += 7)
            {
                ulong b = sliceInput.ReadByteAlt();
                // add the lower 7 bits to the result
                result |= (b & 0x7f) << shift;
                // if high bit is not set, this is the last byte in the number
                if ((b & 0x80) == 0)
                {
                    return result;
                }
            }
            throw new FormatException("last byte of variable length int has high bit set");
        }
    }
}