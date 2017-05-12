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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using LevelDB.Guava;
using LevelDB.Impl;
using LevelDB.Util;
using LevelDB.Util.Extension;

namespace LevelDB.Table
{
    public abstract class Table : ISeekingIterable<Slice, Slice>
    {
        protected static MemoryStream UncompressedScratch = new MemoryStream(4 * 1024 * 1024);

        protected readonly string Name;
        protected readonly FileStream FileChannel;
        protected readonly IComparer<Slice> Comparator;
        protected readonly bool VerifyChecksums;
        protected readonly Block IndexBlock;
        protected readonly BlockHandle MetaindexBlockHandle;

        protected Table(string name, FileStream fileChannel, IComparer<Slice> comparator, bool verifyChecksums)
        {
            Preconditions.CheckNotNull(name, $"{nameof(name)} is null");
            Preconditions.CheckNotNull(fileChannel, $"{nameof(fileChannel)} is null");
            var size = fileChannel.Length;
            Preconditions.CheckArgument(size >= Footer.EncodedLength,
                $"File is corrupt: size must be at least {Footer.EncodedLength} bytes");
            Preconditions.CheckArgument(size <= int.MaxValue, $"File must be smaller than {int.MaxValue} bytes");
            Preconditions.CheckNotNull(comparator, $"{nameof(comparator)} is null");

            Name = name;
            FileChannel = fileChannel;
            VerifyChecksums = verifyChecksums;
            Comparator = comparator;

            var footer = Init();
            IndexBlock = ReadBlock(footer.GetIndexBlockHandle());
            MetaindexBlockHandle = footer.GetMetaindexBlockHandle();
        }

        public TableIterator GetTableIterator()
        {
            return new TableIterator(this, IndexBlock.GetBlockIterator());
        }

        public IEnumerator<Entry<Slice, Slice>> GetEnumerator()
        {
            return GetTableIterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public Block OpenBlock(Slice blockEntry)
        {
            var blockHandle = BlockHandle.ReadBlockHandle(blockEntry.Input());
            var dataBlock = ReadBlock(blockHandle);
            return dataBlock;
        }

        /// <summary>
        /// Given a key, return an approximate byte offset in the file where
        /// the data for that key begins (or would begin if the key were
        /// present in the file).  The returned value is in terms of file
        /// bytes, and so includes effects like compression of the underlying data.
        /// For example, the approximate offset of the last key in the table will
        /// be close to the file length.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public long GetApproximateOffsetOf(Slice key)
        {
            var iterator = IndexBlock.GetBlockIterator();
            iterator.Seek(key);
            // key is past the last key in the file.  Approximate the offset
            // by returning the offset of the metaindex block (which is
            // right near the end of the file).
            if (!iterator.HasNext()) return MetaindexBlockHandle.GetOffset();
            var blockHandle = BlockHandle.ReadBlockHandle(iterator.Next().Value.Input());
            return blockHandle.GetOffset();
        }

        protected int UncompressedLength(MemoryStream data)
        {
            return (int) VariableLengthQuantity.ReadVariableLengthInt(data.Duplicate());
        }

        public Action Closer() => () => Disposables.DisposeQuietly(FileChannel);

        public override string ToString()
        {
            return $"Table(name='{Name}', comparator={Comparator}, verifyChecksums={VerifyChecksums})";
        }

        #region Abstract

        protected abstract Footer Init();

        protected abstract Block ReadBlock(BlockHandle blockHandle);

        #endregion
    }
}