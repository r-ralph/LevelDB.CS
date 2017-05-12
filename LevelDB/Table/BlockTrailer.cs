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
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Table
{
    public class BlockTrailer
    {
        public const int EncodedLength = 5;

        public CompressionType CompressionType { get; }

        public uint Crc32C { get; }

        public BlockTrailer(CompressionType compressionType, uint crc32C)
        {
            Preconditions.CheckNotNull(compressionType, $"{nameof(compressionType)} is null");

            CompressionType = compressionType;
            Crc32C = crc32C;
        }


        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            var that = o as BlockTrailer;
            if (Crc32C != that?.Crc32C)
            {
                return false;
            }
            return CompressionType == that.CompressionType;
        }

        public override int GetHashCode()
        {
            var result = CompressionType.GetHashCode();
            result = (int) (31 * result + Crc32C);
            return result;
        }

        public override string ToString()
        {
            return $"BlockTrailer(compressionType={CompressionType}, crc32c={Convert.ToString(Crc32C, 16)})";
        }

        public static BlockTrailer ReadBlockTrailer(Slice slice)
        {
            var sliceInput = slice.Input();
            var compressionType = CompressionType.GetCompressionTypeByPersistentId(sliceInput.ReadByte());
            var crc32C = sliceInput.ReadUnsignedInt();
            return new BlockTrailer(compressionType, crc32C);
        }

        public static Slice WriteBlockTrailer(BlockTrailer blockTrailer)
        {
            var slice = Slices.Allocate(EncodedLength);
            WriteBlockTrailer(blockTrailer, slice.Output());
            return slice;
        }

        public static void WriteBlockTrailer(BlockTrailer blockTrailer, SliceOutput sliceOutput)
        {
            sliceOutput.WriteByte(blockTrailer.CompressionType.PersistentId);
            sliceOutput.WriteUnsignedInt(blockTrailer.Crc32C);
        }
    }
}