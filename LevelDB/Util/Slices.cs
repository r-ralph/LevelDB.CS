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

using System.IO;
using System.Text;
using LevelDB.Guava;
using LevelDB.Util.Extension;

namespace LevelDB.Util
{
    public static class Slices
    {
        /// <summary>
        /// A buffer whose capacity is <code>0</code>.
        /// </summary>
        public static readonly Slice EmptySlice = new Slice(0);

        public static Slice ReadLengthPrefixedBytes(SliceInput sliceInput)
        {
            var length = VariableLengthQuantity.ReadVariableLengthInt(sliceInput);
            return sliceInput.ReadBytes((int) length);
        }

        public static void WriteLengthPrefixedBytes(SliceOutput sliceOutput, Slice value)
        {
            VariableLengthQuantity.WriteVariableLengthInt((uint) value.Length, sliceOutput);
            sliceOutput.WriteBytes(value);
        }

        public static Slice EnsureSize(Slice existingSlice, int minWritableBytes)
        {
            if (existingSlice == null)
            {
                existingSlice = EmptySlice;
            }

            if (minWritableBytes <= existingSlice.Length)
            {
                return existingSlice;
            }

            int newCapacity;
            newCapacity = existingSlice.Length == 0 ? 1 : existingSlice.Length;
            var minNewCapacity = existingSlice.Length + minWritableBytes;
            while (newCapacity < minNewCapacity)
            {
                newCapacity <<= 1;
            }
            var newSlice = Allocate(newCapacity);
            newSlice.SetBytes(0, existingSlice, 0, existingSlice.Length);
            return newSlice;
        }

        public static Slice Allocate(int capacity)
        {
            return capacity == 0 ? EmptySlice : new Slice(capacity);
        }

        public static Slice WrappedBuffer(byte[] array)
        {
            return array.Length == 0 ? EmptySlice : new Slice(array);
        }

        public static Slice CopiedBuffer(MemoryStream source, int sourceOffset, int length)
        {
            Preconditions.CheckNotNull(source, $"{nameof(source)} is null");
            var newPosition = source.Position + sourceOffset;
            var newMs = source.Duplicate();
            newMs.Position = newPosition;
            newMs.SetLength(length);
            return CopiedBuffer(newMs);
        }

        public static Slice CopiedBuffer(MemoryStream source)
        {
            Preconditions.CheckNotNull(source, $"{nameof(source)} is null");
            var copy = Allocate((int) (source.Length - source.Position));
            copy.SetBytes(0, source.Duplicate());
            return copy;
        }

        public static Slice CopiedBuffer(string str, Encoding charset)
        {
            Preconditions.CheckNotNull(str, $"{str} is null");
            Preconditions.CheckNotNull(charset, $"{charset} is null");
            return WrappedBuffer(charset.GetBytes(str));
        }

        public static string DecodeString(MemoryStream src, Encoding charset)
        {
            return charset.GetString(src.ToArray());
        }
    }
}