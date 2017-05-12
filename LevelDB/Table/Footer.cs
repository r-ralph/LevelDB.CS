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

using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Table
{
    public class Footer
    {
        public const int EncodedLength = BlockHandle.MaxEncodedLength * 2 + SizeOf.Long;

        private readonly BlockHandle _metaindexBlockHandle;
        private readonly BlockHandle _indexBlockHandle;

        protected internal Footer(BlockHandle metaindexBlockHandle, BlockHandle indexBlockHandle)
        {
            _metaindexBlockHandle = metaindexBlockHandle;
            _indexBlockHandle = indexBlockHandle;
        }

        public BlockHandle GetMetaindexBlockHandle()
        {
            return _metaindexBlockHandle;
        }

        public BlockHandle GetIndexBlockHandle()
        {
            return _indexBlockHandle;
        }

        public static Footer ReadFooter(Slice slice)
        {
            Preconditions.CheckNotNull(slice, $"{nameof(slice)} is null");
            Preconditions.CheckArgument(slice.Length == EncodedLength, $"Expected slice.size to be {EncodedLength} but was {slice.Length}");
            var sliceInput = slice.Input();

            // read metaindex and index handles
            var metaindexBlockHandle = BlockHandle.ReadBlockHandle(sliceInput);
            var indexBlockHandle = BlockHandle.ReadBlockHandle(sliceInput);

            // skip padding
            sliceInput.Position = (EncodedLength - SizeOf.Long);

            // verify magic number
            var magicNumber = sliceInput.ReadUnsignedLong();
            Preconditions.CheckArgument(magicNumber == TableBuilder.TableMagicNumber,
                "File is not a table (bad magic number)");
            return new Footer(metaindexBlockHandle, indexBlockHandle);
        }

        public static Slice WriteFooter(Footer footer)
        {
            var slice = Slices.Allocate(EncodedLength);
            WriteFooter(footer, slice.Output());
            return slice;
        }

        public static void WriteFooter(Footer footer, SliceOutput sliceOutput)
        {
            // remember the starting write index so we can calculate the padding
            var startingWriteIndex = sliceOutput.Size();

            // write metaindex and index handles
            BlockHandle.WriteBlockHandleTo(footer.GetMetaindexBlockHandle(), sliceOutput);
            BlockHandle.WriteBlockHandleTo(footer.GetIndexBlockHandle(), sliceOutput);

            // write padding
            sliceOutput.WriteZero((uint) (EncodedLength - SizeOf.Long - (sliceOutput.Size() - startingWriteIndex)));

            // write magic number as two (little endian) integers
            sliceOutput.WriteUnsignedLong(TableBuilder.TableMagicNumber);
        }
    }
}