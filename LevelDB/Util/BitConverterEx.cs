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
using System.Linq;

namespace LevelDB.Util
{
    public static class BitConverterEx
    {
        public static short ToShort(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Short);
            return BitConverter.ToInt16(ReverseIfNeed(sub, endian), 0);
        }

        public static ushort ToUndignedShort(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Short);
            return BitConverter.ToUInt16(ReverseIfNeed(sub, endian), 0);
        }

        public static int ToInt(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Int);
            return BitConverter.ToInt32(ReverseIfNeed(sub, endian), 0);
        }

        public static uint ToUnsignedInt(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Int);
            return BitConverter.ToUInt32(ReverseIfNeed(sub, endian), 0);
        }

        public static long ToLong(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Long);
            return BitConverter.ToInt64(ReverseIfNeed(sub, endian), 0);
        }

        public static ulong ToUnsignedLong(byte[] value, int startIndex, Endian endian)
        {
            var sub = GetSubArray(value, startIndex, SizeOf.Long);
            return BitConverter.ToUInt64(ReverseIfNeed(sub, endian), 0);
        }

        private static byte[] GetSubArray(byte[] src, int startIndex, int count)
        {
            var dst = new byte[count];
            Array.Copy(src, startIndex, dst, 0, count);
            return dst;
        }

        private static byte[] ReverseIfNeed(byte[] bytes, Endian endian)
        {
            if (BitConverter.IsLittleEndian ^ endian == Endian.Little)
            {
                return bytes.Reverse().ToArray();
            }
            return bytes;
        }
    }

    public enum Endian
    {
        Little,
        Big
    }
}