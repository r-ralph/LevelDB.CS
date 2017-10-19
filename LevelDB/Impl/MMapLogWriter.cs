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
using System.IO.MemoryMappedFiles;
using LevelDB.Guava;
using LevelDB.Util;
using LevelDB.Util.Atomic;
using LevelDB.Util.Extension;

namespace LevelDB.Impl
{
    public class MMapLogWriter : ILogWriter
    {
        private const int PageSize = 1024 * 1024;

        public bool IsClosed => _closed.Value;
        public FileInfo File { get; }

        public long FileNumber { get; }

        private readonly AtomicBoolean _closed = new AtomicBoolean();
        private MemoryMappedFile _memoryMappedFile;
        private UnmanagedMemoryStream _mappedByteBuffer;
        private long _fileOffset;

        /**
         * Current offset in the current block
         */
        private int _blockOffset;

        public MMapLogWriter(FileInfo file, long fileNumber)
        {
            Preconditions.CheckNotNull(file, $"{nameof(file)} is null");
            Preconditions.CheckArgument(fileNumber >= 0, $"{nameof(fileNumber)} is negative");

            File = file;
            FileNumber = fileNumber;
            _memoryMappedFile = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.OpenOrCreate, null, PageSize);
            _mappedByteBuffer = _memoryMappedFile.CreateViewStream(0, PageSize, MemoryMappedFileAccess.ReadWrite);
        }

        public void Close()
        {
            _closed.GetAndSet(true);

            DestroyMappedByteBuffer();

            // close the channel
            Disposables.DisposeQuietly(_memoryMappedFile);

            using (var stream = File.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                stream.SetLength(_fileOffset);
            }
        }

        public void Delete()
        {
            Close();

            // try to delete the file
            File.Delete();
        }

        private void DestroyMappedByteBuffer()
        {
            if (_mappedByteBuffer != null)
            {
                _fileOffset += _mappedByteBuffer.Position;
                Unmap();
            }
            _mappedByteBuffer = null;
        }

        public void AddRecord(Slice record, bool force)
        {
            Preconditions.CheckState(!_closed.Value, "Log has been closed");

            var sliceInput = record.Input();

            // used to track first, middle and last blocks
            var begin = true;

            // Fragment the record int chunks as necessary and write it.  Note that if record
            // is empty, we still want to iterate once to write a single
            // zero-length chunk.
            do
            {
                var bytesRemainingInBlock = LogConstants.BlockSize - _blockOffset;
                Preconditions.CheckState(bytesRemainingInBlock >= 0);

                // Switch to a new block if necessary
                if (bytesRemainingInBlock < LogConstants.HeaderSize)
                {
                    if (bytesRemainingInBlock > 0)
                    {
                        // Fill the rest of the block with zeros
                        // todo lame... need a better way to write zeros
                        EnsureCapacity(bytesRemainingInBlock);
                        _mappedByteBuffer.Write(new byte[bytesRemainingInBlock], 0, bytesRemainingInBlock);
                    }
                    _blockOffset = 0;
                    bytesRemainingInBlock = LogConstants.BlockSize - _blockOffset;
                }

                // Invariant: we never leave less than HEADER_SIZE bytes available in a block
                var bytesAvailableInBlock = bytesRemainingInBlock - LogConstants.HeaderSize;
                Preconditions.CheckState(bytesAvailableInBlock >= 0);

                // if there are more bytes in the record then there are available in the block,
                // fragment the record; otherwise write to the end of the record
                bool end;
                int fragmentLength;
                if (sliceInput.Available > bytesAvailableInBlock)
                {
                    end = false;
                    fragmentLength = bytesAvailableInBlock;
                }
                else
                {
                    end = true;
                    fragmentLength = sliceInput.Available;
                }

                // determine block type
                LogChunkType type;
                if (begin && end)
                {
                    type = LogChunkType.Full;
                }
                else if (begin)
                {
                    type = LogChunkType.First;
                }
                else if (end)
                {
                    type = LogChunkType.Last;
                }
                else
                {
                    type = LogChunkType.Middle;
                }

                // write the chunk
                WriteChunk(type, sliceInput.ReadBytes(fragmentLength));

                // we are no longer on the first chunk
                begin = false;
            } while (sliceInput.CanRead);

            if (force)
            {
                _mappedByteBuffer.Flush();
            }
        }

        private void WriteChunk(LogChunkType type, Slice slice)
        {
            Preconditions.CheckArgument(slice.Length <= 0xffff, $"length {slice.Length} is larger than two bytes");
            Preconditions.CheckArgument(_blockOffset + LogConstants.HeaderSize <= LogConstants.BlockSize);

            // create header
            var header = NewLogRecordHeader(type, slice);

            // write the header and the payload
            EnsureCapacity(header.Length + slice.Length);
            header.GetBytes(0, _mappedByteBuffer);
            slice.GetBytes(0, _mappedByteBuffer);

            _blockOffset += LogConstants.HeaderSize + slice.Length;
        }

        private void EnsureCapacity(int bytes)
        {
            if (_mappedByteBuffer.Remaining() < bytes)
            {
                // remap
                _fileOffset += _mappedByteBuffer.Position;
                Unmap();

                _memoryMappedFile = MemoryMappedFile.CreateFromFile(File.FullName, FileMode.OpenOrCreate, null,
                    _fileOffset + PageSize);
                _mappedByteBuffer =
                    _memoryMappedFile.CreateViewStream(_fileOffset, PageSize, MemoryMappedFileAccess.ReadWrite);
            }
        }

        private void Unmap()
        {
            Disposables.DisposeQuietly(_mappedByteBuffer);
            Disposables.DisposeQuietly(_memoryMappedFile);
        }

        private static Slice NewLogRecordHeader(LogChunkType type, Slice slice)
        {
            var crc = Logs.GetChunkChecksum(type.PersistentId, slice.GetRawArray(), slice.GetRawOffset(), slice.Length);

            // Format the header
            var header = Slices.Allocate(LogConstants.HeaderSize);
            var sliceOutput = header.Output();
            sliceOutput.WriteInt((int) crc);
            sliceOutput.WriteByte((byte) (slice.Length & 0xff));
            sliceOutput.WriteByte((byte) ((uint) slice.Length >> 8));
            sliceOutput.WriteByte((byte) type.PersistentId);

            return header;
        }
    }
}