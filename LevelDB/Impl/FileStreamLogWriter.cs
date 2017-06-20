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
using LevelDB.Guava;
using LevelDB.Util;
using LevelDB.Util.Atomic;
using LevelDB.Util.Extension;
using static LevelDB.Impl.LogConstants;

namespace LevelDB.Impl
{
    public class FileStreamLogWriter : ILogWriter
    {
        public FileInfo File { get; }
        public long FileNumber { get; }
        public bool IsClosed => _closed.Value;

        private readonly FileStream _fileStream;
        private readonly AtomicBoolean _closed = new AtomicBoolean();
        private readonly object _syncClose = new object();
        private readonly object _syncDelete = new object();
        private readonly object _syncAddRecord = new object();

        /// <summary>
        /// Current offset in the current block
        /// </summary>
        private int _blockOffset;

        public FileStreamLogWriter(FileInfo file, long fileNumber)
        {
            Preconditions.CheckNotNull(file, $"{nameof(file)} is null");
            Preconditions.CheckArgument(fileNumber >= 0, $"{nameof(fileNumber)} is negative");

            File = file;
            FileNumber = fileNumber;
            _fileStream = file.Open(FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite);
        }

        public void Close()
        {
            lock (_syncClose)
            {
                _closed.GetAndSet(true);
                // try to forces the log to disk
                try
                {
                    _fileStream.Flush(true);
                }
                catch (IOException)
                {
                }

                // close the channel
                Disposables.DisposeQuietly(_fileStream);
            }
        }

        public void Delete()
        {
            lock (_syncDelete)
            {
                _closed.GetAndSet(true);

                // close the channel
                Disposables.DisposeQuietly(_fileStream);

                // try to delete the file
                File.Delete();
            }
        }

        public void AddRecord(Slice record, bool force)
        {
            lock (_syncAddRecord)
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
                    var bytesRemainingInBlock = BlockSize - _blockOffset;
                    Preconditions.CheckState(bytesRemainingInBlock >= 0);

                    // Switch to a new block if necessary
                    if (bytesRemainingInBlock < HeaderSize)
                    {
                        if (bytesRemainingInBlock > 0)
                        {
                            // Fill the rest of the block with zeros
                            // todo lame... need a better way to write zeros
                            _fileStream.Fill(0, bytesRemainingInBlock);
                        }
                        _blockOffset = 0;
                        bytesRemainingInBlock = BlockSize - _blockOffset;
                    }

                    // Invariant: we never leave less than HEADER_SIZE bytes available in a block
                    var bytesAvailableInBlock = bytesRemainingInBlock - HeaderSize;
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
                    WriteChunk(type, sliceInput.ReadSlice(fragmentLength));

                    // we are no longer on the first chunk
                    begin = false;
                } while (sliceInput.CanRead);

                if (force)
                {
                    _fileStream.Flush(false);
                }
            }
        }

        private void WriteChunk(LogChunkType type, Slice slice)
        {
            Preconditions.CheckArgument(slice.Length <= 0xffff, $"length {slice.Length} is larger than two bytes");
            Preconditions.CheckArgument(_blockOffset + HeaderSize <= BlockSize);

            // create header
            var header = NewLogRecordHeader(type, slice, slice.Length);

            // write the header and the payload
            header.GetBytes(0, _fileStream, header.Length);
            slice.GetBytes(0, _fileStream, slice.Length);

            _blockOffset += HeaderSize + slice.Length;
        }

        private Slice NewLogRecordHeader(LogChunkType type, Slice slice, int length)
        {
            var crc = Logs.GetChunkChecksum(type.PersistentId, slice.GetRawArray(), slice.GetRawOffset(), length);
            var ulength = (uint) length;
            // Format the header
            var header = Slices.Allocate(HeaderSize).Output();
            header.WriteInt((int) crc);
            header.WriteByte((byte) (ulength & 0xff));
            header.WriteByte((byte) (ulength >> 8));
            header.WriteByte((byte) type.PersistentId);

            return header.Sliced();
        }
    }
}