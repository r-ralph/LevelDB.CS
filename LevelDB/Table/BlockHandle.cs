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
using LevelDB.Util;

namespace LevelDB.Table
{
    public class BlockHandle
    {
        public const int MaxEncodedLength = 10 + 10;

        private readonly long _offset;
        private readonly int _dataSize;

        internal BlockHandle(long offset, int dataSize)
        {
            _offset = offset;
            _dataSize = dataSize;
        }

        public long GetOffset()
        {
            return _offset;
        }

        public int GetDataSize()
        {
            return _dataSize;
        }

        public int GetFullBlockSize()
        {
            return _dataSize + BlockTrailer.EncodedLength;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            var that = o as BlockHandle;
            if (_dataSize != that?._dataSize)
            {
                return false;
            }
            return _offset == that._offset;
        }

        public override int GetHashCode()
        {
            var result = (int) (_offset ^ (_offset >> 32));
            result = 31 * result + _dataSize;
            return result;
        }

        public override string ToString()
        {
            return $"BlockHandle(offset={_offset}, dataSize={_dataSize})";
        }

        public static BlockHandle ReadBlockHandle(SliceInput sliceInput)
        {
            var offset = VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
            var size = VariableLengthQuantity.ReadVariableLengthLong(sliceInput);

            if (size > int.MaxValue)
            {
                throw new ArgumentException("Blocks can not be larger than Integer.MAX_VALUE");
            }

            return new BlockHandle((long) offset, (int) size);
        }

        public static Slice WriteBlockHandle(BlockHandle blockHandle)
        {
            var slice = Slices.Allocate(MaxEncodedLength);
            var sliceOutput = slice.Output();
            WriteBlockHandleTo(blockHandle, sliceOutput);
            return slice.Sliced();
        }

        public static void WriteBlockHandleTo(BlockHandle blockHandle, SliceOutput sliceOutput)
        {
            VariableLengthQuantity.WriteVariableLengthLong((ulong) blockHandle._offset, sliceOutput);
            VariableLengthQuantity.WriteVariableLengthLong((ulong) blockHandle._dataSize, sliceOutput);
        }
    }
}