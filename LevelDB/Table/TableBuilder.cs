using System;
using System.Diagnostics;
using System.IO;
using LevelDB.Guava;
using LevelDB.Util;
using Snappy.Sharp;
using static LevelDB.Impl.VersionSet;

namespace LevelDB.Table
{
    public class TableBuilder
    {
        /// <summary>
        /// TableMagicNumber was picked by running
        /// echo http://code.google.com/p/leveldb/ | sha1sum
        /// and taking the leading 64 bits.
        /// </summary>
        public const ulong TableMagicNumber = 0xdb4775248b80fb57L;

        private readonly int _blockRestartInterval;
        private readonly int _blockSize;
        private readonly CompressionType _compressionType;

        private readonly FileStream _fileStream;
        private readonly BlockBuilder _dataBlockBuilder;
        private readonly BlockBuilder _indexBlockBuilder;
        private Slice _lastKey;
        private readonly IUserComparator _userComparator;

        private long _entryCount;

        // Either Finish() or Abandon() has been called.
        private bool _closed;

        // We do not emit the index entry for a block until we have seen the
        // first key for the next data block.  This allows us to use shorter
        // keys in the index block.  For example, consider a block boundary
        // between the keys "the quick brown fox" and "the who".  We can use
        // "the r" as the key for the index block entry since it is >= all
        // entries in the first block and < all entries in subsequent
        // blocks.
        private bool _pendingIndexEntry;

        private BlockHandle _pendingHandle; // Handle to add to index block

        private Slice _compressedOutput;

        private long _position;

        public TableBuilder(Options options, FileStream fileStream, IUserComparator userComparator)
        {
            Preconditions.CheckNotNull(options, $"{nameof(options)} is null");
            Preconditions.CheckNotNull(fileStream, $"{nameof(fileStream)} is null");
            Preconditions.CheckState(_position == fileStream.Position,
                $"Expected position {_position} to equal fileStream.position ${fileStream.Position}");

            _fileStream = fileStream;
            _userComparator = userComparator;

            _blockRestartInterval = options.BlockRestartInterval();
            _blockSize = options.BlockSize();
            _compressionType = options.CompressionType();

            _dataBlockBuilder = new BlockBuilder((int) Math.Min(_blockSize * 1.1, TargetFileSize),
                _blockRestartInterval,
                userComparator);

            // with expected 50% compression
            var expectedNumberOfBlocks = 1024;
            _indexBlockBuilder = new BlockBuilder(BlockHandle.MaxEncodedLength * expectedNumberOfBlocks, 1,
                userComparator);

            _lastKey = Slices.EmptySlice;
        }

        public long GetEntryCount()
        {
            return _entryCount;
        }

        public long GetFileSize()
        {
            return _position + _dataBlockBuilder.CurrentSizeEstimate();
        }

        public void Add(BlockEntry blockEntry)
        {
            Preconditions.CheckNotNull(blockEntry, $"{nameof(blockEntry)} is null");
            Add(blockEntry.Key, blockEntry.Value);
        }

        public void Add(Slice key, Slice value)
        {
            Preconditions.CheckNotNull(key, $"{nameof(key)} is null");
            Preconditions.CheckNotNull(value, $"{nameof(value)} is null");
            Preconditions.CheckState(!_closed, "tabel is finished");
            if (_entryCount > 0)
            {
                Debug.Assert(_userComparator.Compare(key, _lastKey) > 0); // key must be greater than last key
            }

            // If we just wrote a block, we can now add the handle to index block
            if (_pendingIndexEntry)
            {
                Preconditions.CheckState(_dataBlockBuilder.IsEmpty(),
                    "Internal error: Table has a pending index entry but data block builder is empty");

                var shortestSeparator = _userComparator.FindShortestSeparator(_lastKey, key);

                var handleEncoding = BlockHandle.WriteBlockHandle(_pendingHandle);
                _indexBlockBuilder.Add(shortestSeparator, handleEncoding);
                _pendingIndexEntry = false;
            }

            _lastKey = key;
            _entryCount++;
            _dataBlockBuilder.Add(key, value);

            var estimatedBlockSize = _dataBlockBuilder.CurrentSizeEstimate();
            if (estimatedBlockSize >= _blockSize)
            {
                Flush();
            }
        }

        private void Flush()
        {
            Preconditions.CheckState(!_closed, "table is finished");
            if (_dataBlockBuilder.IsEmpty())
            {
                return;
            }
            Preconditions.CheckState(!_pendingIndexEntry,
                "Internal error: Table already has a pending index entry to flush");
            _pendingHandle = WriteBlock(_dataBlockBuilder);
            _pendingIndexEntry = true;
        }

        private BlockHandle WriteBlock(BlockBuilder blockBuilder)
        {
            // close the block
            var raw = blockBuilder.Finish();

            // attempt to compress the block
            var blockContents = raw;

            var blockCompressionType = CompressionType.None;
            /**
            if (compressionType == CompressionType.Z)
            {
                ensureCompressedOutputCapacity(maxCompressedLength(raw.length()));
                try
                {
                    int compressedSize = Zlib.compress(raw.getRawArray(), raw.getRawOffset(), raw.length(),
                        compressedOutput.getRawArray(), 0);

                    // Don't use the compressed data if compressed less than 12.5%,
                    if (compressedSize < raw.length() - (raw.length() / 8))
                    {
                        blockContents = compressedOutput.slice(0, compressedSize);
                        blockCompressionType = CompressionType.ZLIB;
                    }
                }
                catch (IOException ignored)
                {
                    // compression failed, so just store uncompressed form
                }
            } else
            **/
            if (_compressionType == CompressionType.Snappy)
            {
                EnsureCompressedOutputCapacity(MaxCompressedLength(raw.Length));
                try
                {
                    var compressedSize = new SnappyCompressor().Compress(raw.GetRawArray(), raw.GetRawOffset(),
                        raw.Length, _compressedOutput.GetRawArray(), 0);

                    // Don't use the compressed data if compressed less than 12.5%,
                    if (compressedSize < raw.Length - raw.Length / 8)
                    {
                        blockContents = _compressedOutput.Sliced(0, compressedSize);
                        blockCompressionType = CompressionType.Snappy;
                    }
                }
                catch (IOException)
                {
                    // compression failed, so just store uncompressed form
                }
            }

            // create block trailer
            var blockTrailer =
                new BlockTrailer(blockCompressionType, Crc32C(blockContents, blockCompressionType));
            var trailer = BlockTrailer.WriteBlockTrailer(blockTrailer);

            // create a handle to this block
            var blockHandle = new BlockHandle(_position, blockContents.Length);

            // write data and trailer
            var blockContentsStream = blockContents.ToMemoryStream();
            blockContentsStream.CopyTo(_fileStream);
            _position += blockContentsStream.Length;
            var trailerStream = trailer.ToMemoryStream();
            trailerStream.CopyTo(_fileStream);
            _position += trailerStream.Length;

            // clean up state
            blockBuilder.Reset();
            return blockHandle;
        }

        private static int MaxCompressedLength(int length)
        {
            // Compressed data can be defined as:
            //    compressed := item* literal*
            //    item       := literal* copy
            //
            // The trailing literal sequence has a space blowup of at most 62/60
            // since a literal of length 60 needs one tag byte + one extra byte
            // for length information.
            //
            // Item blowup is trickier to measure.  Suppose the "copy" op copies
            // 4 bytes of data.  Because of a special check in the encoding code,
            // we produce a 4-byte copy only if the offset is < 65536.  Therefore
            // the copy op takes 3 bytes to encode, and this type of item leads
            // to at most the 62/60 blowup for representing literals.
            //
            // Suppose the "copy" op copies 5 bytes of data.  If the offset is big
            // enough, it will take 5 bytes to encode the copy op.  Therefore the
            // worst case here is a one-byte literal followed by a five-byte copy.
            // I.e., 6 bytes of input turn into 7 bytes of "compressed" data.
            //
            // This last factor dominates the blowup, so the readonly estimate is:
            return 32 + length + (length / 6);
        }

        public void Finish()
        {
            Preconditions.CheckState(!_closed, "table is finished");

            // flush current data block
            Flush();

            // mark table as closed
            _closed = true;

            // write (empty) meta index block
            var metaIndexBlockBuilder = new BlockBuilder(256, _blockRestartInterval, new BytewiseComparator());

            // TODO(postrelease): Add stats and other meta blocks
            var metaindexBlockHandle = WriteBlock(metaIndexBlockBuilder);

            // add last handle to index block
            if (_pendingIndexEntry)
            {
                var shortSuccessor = _userComparator.FindShortSuccessor(_lastKey);
                var handleEncoding = BlockHandle.WriteBlockHandle(_pendingHandle);
                _indexBlockBuilder.Add(shortSuccessor, handleEncoding);
                _pendingIndexEntry = false;
            }

            // write index block
            var indexBlockHandle = WriteBlock(_indexBlockBuilder);

            // write footer
            var footer = new Footer(metaindexBlockHandle, indexBlockHandle);

            var footerEncodingStream = Footer.WriteFooter(footer).ToMemoryStream();
            footerEncodingStream.CopyTo(_fileStream);
            _position += footerEncodingStream.Length;
        }

        public void Abandon()
        {
            Preconditions.CheckState(!_closed, "table is finished");
            _closed = true;
        }

        public void EnsureCompressedOutputCapacity(int capacity)
        {
            if (_compressedOutput != null && _compressedOutput.Length > capacity)
            {
                return;
            }
            _compressedOutput = Slices.Allocate(capacity);
        }

        public static uint Crc32C(Slice data, CompressionType type)
        {
            var crc32C = new Crc32C.Sharp.Crc32C();
            crc32C.Update(data.GetRawArray(), data.GetRawOffset(), data.Length);
            crc32C.Update(type.PersistentId & 0xFF);
            return crc32C.GetMaskedValue();
        }
    }
}