﻿#region Copyright

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

using System.Collections.Generic;
using System.IO;
using LevelDB.Util;
using LevelDB.Util.Extension;
using Snappy.Sharp;

namespace LevelDB.Table
{
    public class FileChannelTable : Table
    {
        private static readonly object SyncLock = new object();

        public FileChannelTable(string name, FileStream fileStream, IComparer<Slice> comparator,
            bool verifyChecksums) : base(name, fileStream, comparator, verifyChecksums)
        {
        }

        protected override Footer Init()
        {
            var size = FileChannel.Length;
            var footerData = Read(size - Footer.EncodedLength, Footer.EncodedLength);
            return Footer.ReadFooter(Slices.CopiedBuffer(footerData));
        }

        protected override Block ReadBlock(BlockHandle blockHandle)
        {
            // read block trailer
            var trailerData = Read(blockHandle.GetOffset() + blockHandle.GetDataSize(),
                BlockTrailer.EncodedLength);
            var blockTrailer = BlockTrailer.ReadBlockTrailer(Slices.CopiedBuffer(trailerData));

            // todo re-enable crc check when ported to support direct buffers
            // // only verify check sums if explicitly asked by the user
            // if (verifyChecksums) {
            //     // checksum data and the compression type in the trailer
            //     PureJavaCrc32C checksum = new PureJavaCrc32C();
            //     checksum.update(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize() + 1);
            //     int actualCrc32c = checksum.getMaskedValue();
            //
            //     Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
            // }

            // decompress data
            var uncompressedBuffer = Read(blockHandle.GetOffset(), blockHandle.GetDataSize());
            Slice uncompressedData;
            /*
            if (blockTrailer.getCompressionType() == ZLIB)
            {
                synchronized(FileChannelTable.class) {
                    int uncompressedLength = uncompressedLength(uncompressedBuffer);
                    if (uncompressedScratch.capacity() < uncompressedLength)
                    {
                        uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
                    }
                    uncompressedScratch.clear();

                    Zlib.uncompress(uncompressedBuffer, uncompressedScratch);
                    uncompressedData = Slices.copiedBuffer(uncompressedScratch);
                }
            }
            else*/
            if (blockTrailer.CompressionType == CompressionType.Snappy)
            {
                lock (SyncLock)
                {
                    var uncompressedLength = UncompressedLength(uncompressedBuffer);
                    if (UncompressedScratch.Capacity < uncompressedLength)
                    {
                        UncompressedScratch = new MemoryStream(uncompressedLength);
                    }
                    UncompressedScratch.Clear();
                    var decompress =
                        new SnappyDecompressor().Decompress(uncompressedBuffer.ToArray(), 0, uncompressedLength);
                    UncompressedScratch.Write(decompress, 0, decompress.Length);
                    uncompressedData = Slices.CopiedBuffer(UncompressedScratch);
                }
            }
            else
            {
                uncompressedData = Slices.CopiedBuffer(uncompressedBuffer);
            }

            return new Block(uncompressedData, Comparator);
        }

        private MemoryStream Read(long offset, int length)
        {
            var uncompressedBuffer = new MemoryStream(length);
            FileChannel.Seek(offset, SeekOrigin.Begin);
            FileChannel.CopyTo(uncompressedBuffer);
            if (uncompressedBuffer.Remaining() >= 2)
            {
                throw new IOException("Could not read all the data");
            }
            uncompressedBuffer.Seek(0, SeekOrigin.Begin);
            return uncompressedBuffer;
        }
    }
}