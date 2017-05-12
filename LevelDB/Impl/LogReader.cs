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
using LevelDB.Util;
using static LevelDB.Impl.LogChunkType;
using static LevelDB.Impl.LogConstants;

namespace LevelDB.Impl
{
    public class LogReader
    {
        private readonly FileStream _fileChannel;

        private readonly LogMonitor _monitor;

        private readonly bool _verifyChecksums;

        /// <summary>
        /// Offset at which to start looking for the first record to return
        /// </summary>
        private readonly long _initialOffset;

        /// <summary>
        /// Have we read to the end of the file?
        /// </summary>
        private bool _eof;

        /// <summary>
        /// Offset of the first location past the end of buffer.
        /// </summary>
        private long _endOfBufferOffset;

        /// <summary>
        /// Scratch buffer in which the next record is assembled.
        /// </summary>
        private readonly DynamicSliceOutput _recordScratch = new DynamicSliceOutput(BlockSize);

        /// <summary>
        /// Scratch buffer for current block.  The currentBlock is sliced off the underlying buffer.
        /// </summary>
        private readonly SliceOutput _blockScratch = Slices.Allocate(BlockSize).Output();

        /// <summary>
        /// The current block records are being read from.
        /// </summary>
        private SliceInput _currentBlock = Slices.EmptySlice.Input();

        /// <summary>
        /// Current chunk which is sliced from the current block.
        /// </summary>
        private Slice _currentChunk = Slices.EmptySlice;

        /// <summary>
        /// Offset of the last record returned by readRecord.
        /// </summary>
        public long LastRecordOffset { get; private set; }

        public LogReader(FileStream fileChannel, LogMonitor monitor, bool verifyChecksums, long initialOffset)
        {
            _fileChannel = fileChannel;
            _monitor = monitor;
            _verifyChecksums = verifyChecksums;
            _initialOffset = initialOffset;
        }

        /// <summary>
        /// Skips all blocks that are completely before "initial_offset_".
        /// Handles reporting corruption
        /// </summary>
        /// <returns>true on success</returns>
        private bool SkipToInitialBlock()
        {
            var offsetInBlock = (int) (_initialOffset % BlockSize);
            var blockStartLocation = _initialOffset - offsetInBlock;

            // Don't search a block if we'd be in the trailer
            if (offsetInBlock > BlockSize - 6)
            {
                blockStartLocation += BlockSize;
            }

            _endOfBufferOffset = blockStartLocation;

            // Skip to start of first block that can contain the initial record
            if (blockStartLocation <= 0) return true;
            try
            {
                _fileChannel.Position = blockStartLocation;
            }
            catch (IOException e)
            {
                ReportDrop(blockStartLocation, e);
                return false;
            }

            return true;
        }

        public Slice ReadRecord()
        {
            _recordScratch.Reset();

            // advance to the first record, if we haven't already
            if (LastRecordOffset < _initialOffset)
            {
                if (!SkipToInitialBlock())
                {
                    return null;
                }
            }

            // Record offset of the logical record that we're reading
            long prospectiveRecordOffset = 0;

            var inFragmentedRecord = false;
            while (true)
            {
                var physicalRecordOffset = _endOfBufferOffset - _currentChunk.Length;
                var chunkType = ReadNextChunk();
                if (chunkType == Full)
                {
                    if (inFragmentedRecord)
                    {
                        ReportCorruption(_recordScratch.Size(), "Partial record without end");
                        // simply return this full block
                    }
                    _recordScratch.Reset();
                    prospectiveRecordOffset = physicalRecordOffset;
                    LastRecordOffset = prospectiveRecordOffset;
                    return _currentChunk.CopySlice();
                }
                if (chunkType == First)
                {
                    if (inFragmentedRecord)
                    {
                        ReportCorruption(_recordScratch.Size(), "Partial record without end");
                        // clear the scratch and start over from this chunk
                        _recordScratch.Reset();
                    }
                    prospectiveRecordOffset = physicalRecordOffset;
                    _recordScratch.WriteBytes(_currentChunk);
                    inFragmentedRecord = true;
                }
                else if (chunkType == Middle)
                {
                    if (!inFragmentedRecord)
                    {
                        ReportCorruption(_recordScratch.Size(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        _recordScratch.Reset();
                    }
                    else
                    {
                        _recordScratch.WriteBytes(_currentChunk);
                    }
                }
                else if (chunkType == Last)
                {
                    if (!inFragmentedRecord)
                    {
                        ReportCorruption(_recordScratch.Size(), "Missing start of fragmented record");

                        // clear the scratch and skip this chunk
                        _recordScratch.Reset();
                    }
                    else
                    {
                        _recordScratch.WriteBytes(_currentChunk);
                        LastRecordOffset = prospectiveRecordOffset;
                        return _recordScratch.Sliced().CopySlice();
                    }
                }
                else if (chunkType == Eof)
                {
                    if (!inFragmentedRecord) return null;
                    ReportCorruption(_recordScratch.Size(), "Partial record without end");
                    // clear the scratch and return
                    _recordScratch.Reset();
                    return null;
                }
                else if (chunkType == BadChunk)
                {
                    if (!inFragmentedRecord) continue;
                    ReportCorruption(_recordScratch.Size(), "Error in middle of record");
                    inFragmentedRecord = false;
                    _recordScratch.Reset();
                }
                else
                {
                    var dropSize = _currentChunk.Length;
                    if (inFragmentedRecord)
                    {
                        dropSize += _recordScratch.Size();
                    }
                    ReportCorruption(dropSize, $"Unexpected chunk type {chunkType}");
                    inFragmentedRecord = false;
                    _recordScratch.Reset();
                }
            }
        }

        /// <summary>
        /// Return type, or one of the preceding special values
        /// </summary>
        /// <returns></returns>
        private LogChunkType ReadNextChunk()
        {
            // clear the current chunk
            _currentChunk = Slices.EmptySlice;

            // read the next block if necessary
            if (_currentBlock.Available < HeaderSize)
            {
                if (!ReadNextBlock())
                {
                    if (_eof)
                    {
                        return Eof;
                    }
                }
            }

            // parse header
            var expectedChecksum = _currentBlock.ReadUnsignedInt();
            int length = _currentBlock.ReadByteAlt();
            length = length | _currentBlock.ReadByteAlt() << 8;
            var chunkTypeId = _currentBlock.ReadByteAlt();
            var chunkType = GetLogChunkTypeByPersistentId(chunkTypeId);

            // verify length
            if (length > _currentBlock.Available)
            {
                var dropSize = _currentBlock.Available + HeaderSize;
                ReportCorruption(dropSize, "Invalid chunk length");
                _currentBlock = Slices.EmptySlice.Input();
                return BadChunk;
            }

            // skip zero length records
            if (chunkType == ZeroType && length == 0)
            {
                // Skip zero length record without reporting any drops since
                // such records are produced by the writing code.
                _currentBlock = Slices.EmptySlice.Input();
                return BadChunk;
            }

            // Skip physical record that started before initialOffset
            if (_endOfBufferOffset - HeaderSize - length < _initialOffset)
            {
                _currentBlock.SkipBytes(length);
                return BadChunk;
            }

            // read the chunk
            _currentChunk = _currentBlock.ReadBytes(length);

            if (_verifyChecksums)
            {
                var actualChecksum = Logs.GetChunkChecksum(chunkTypeId, _currentChunk);
                if (actualChecksum != expectedChecksum)
                {
                    // Drop the rest of the buffer since "length" itself may have
                    // been corrupted and if we trust it, we could find some
                    // fragment of a real log record that just happens to look
                    // like a valid log record.
                    var dropSize = _currentBlock.Available + HeaderSize;
                    _currentBlock = Slices.EmptySlice.Input();
                    ReportCorruption(dropSize, "Invalid chunk checksum");
                    return BadChunk;
                }
            }

            // Skip unknown chunk types
            // Since this comes last so we the, know it is a valid chunk, and is just a type we don't understand
            if (chunkType != Unknown) return chunkType;
            ReportCorruption(length, $"Unknown chunk type {chunkType.PersistentId}");
            return BadChunk;
        }

        public bool ReadNextBlock()
        {
            if (_eof)
            {
                return false;
            }

            // clear the block
            _blockScratch.Reset();

            // read the next full block
            while (_blockScratch.WritableBytes() > 0)
            {
                try
                {
                    var bytesRead = _blockScratch.WriteBytes(_fileChannel, _blockScratch.WritableBytes());
                    if (bytesRead < 0)
                    {
                        // no more bytes to read
                        _eof = true;
                        break;
                    }
                    _endOfBufferOffset += bytesRead;
                }
                catch (IOException e)
                {
                    _currentBlock = Slices.EmptySlice.Input();
                    ReportDrop(BlockSize, e);
                    _eof = true;
                    return false;
                }
            }
            _currentBlock = _blockScratch.Sliced().Input();
            return _currentBlock.CanRead;
        }

        /// <summary>
        /// Reports corruption to the monitor.
        /// The buffer must be updated to remove the dropped bytes prior to invocation.
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="reason"></param>
        private void ReportCorruption(long bytes, string reason)
        {
            _monitor?.Corruption(bytes, reason);
        }

        /// <summary>
        /// Reports dropped bytes to the monitor.
        /// The buffer must be updated to remove the dropped bytes prior to invocation.
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="reason"></param>
        private void ReportDrop(long bytes, Exception reason)
        {
            _monitor?.Corruption(bytes, reason);
        }
    }
}