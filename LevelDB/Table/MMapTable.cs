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

using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using LevelDB.Guava;
using LevelDB.Util;
using LevelDB.Util.Extension;

namespace LevelDB.Table
{
    public class MMapTable : Table
    {
        private static readonly object SyncLock = new object();

        private MemoryMappedFile _mappedFile;
        private MemoryMappedViewStream _data;

        public MMapTable(string name, FileStream fileChannel, IComparer<Slice> comparator, bool verifyChecksums) :
            base(name, fileChannel, comparator, verifyChecksums)
        {
            Preconditions.CheckArgument(fileChannel.Length <= int.MaxValue, "File must be smaller than {1} bytes",
                int.MaxValue);
        }

        protected override Footer Init()
        {
            var size = FileChannel.Length;
            _mappedFile = MemoryMappedFile.CreateFromFile(FileChannel, null, size,
                MemoryMappedFileAccess.Read, HandleInheritability.Inheritable, false);
            _data = _mappedFile.CreateViewStream(0, size, MemoryMappedFileAccess.Read);
            var footerSlice = Slices.CopiedBuffer(_data, size - Footer.EncodedLength, Footer.EncodedLength);
            return Footer.ReadFooter(footerSlice);
        }

        protected override Block ReadBlock(BlockHandle blockHandle)
        {
            // read block trailer
            var trailerData = Slices.CopiedBuffer(_data, blockHandle.GetOffset() + blockHandle.GetDataSize(),
                BlockTrailer.EncodedLength);
            var blockTrailer = BlockTrailer.ReadBlockTrailer(trailerData);

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
            Slice uncompressedData;
            var compressedStream = Read(_data, blockHandle.GetOffset(), blockHandle.GetDataSize());
            if (blockTrailer.CompressionType == CompressionType.None)
            {
                uncompressedData = Slices.CopiedBuffer(compressedStream);
            }
            else
            {
                lock (SyncLock)
                {
                    var uncompressedLength = UncompressedLength(compressedStream);
                    if (UncompressedScratch.Capacity < uncompressedLength)
                    {
                        UncompressedScratch = new MemoryStream(uncompressedLength);
                    }
                    UncompressedScratch.Clear();
                    Compressions.Decompress(blockTrailer.CompressionType, compressedStream, UncompressedScratch);
                    uncompressedData = Slices.CopiedBuffer(UncompressedScratch);
                }
            }

            return new Block(uncompressedData, Comparator);
        }

        public override void Dispose()
        {
            Disposables.DisposeQuietly(_data);
            Disposables.DisposeQuietly(_mappedFile);
            base.Dispose();
        }

        private MemoryStream Read(MemoryMappedViewStream data, long offset, int length)
        {
            var newPosition = data.Position + offset;
            var newMs = data.Duplicate();
            newMs.SetLength(newPosition + length);
            newMs.Position = newPosition;
            return newMs;
        }
    }
}