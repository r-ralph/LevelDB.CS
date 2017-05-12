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
using System.Text;
using LevelDB.Util.Extension;

namespace LevelDB.Util
{
    public class BasicSliceOutput : SliceOutput
    {
        private readonly Slice _slice;
        private int _size;

        public override bool CanWrite => WritableBytes() > 0;

        public override long Length => _size;

        public override long Position
        {
            get => _size;
            set { }
        }

        public override bool CanRead => false;

        public override bool CanSeek => false;

        public BasicSliceOutput(Slice slice)
        {
            _slice = slice;
        }

        public override void Reset()
        {
            _size = 0;
        }

        public override int Size()
        {
            return _size;
        }

        public override int WritableBytes()
        {
            return _slice.Length - _size;
        }

        public override void WriteByte(byte value)
        {
            _slice.SetByte(_size, value);
            _size += SizeOf.Byte;
        }


        public override void WriteSignedByte(sbyte value)
        {
            _slice.SetSignedByte(_size, value);
            _size += SizeOf.Byte;
        }

        public override void WriteShort(short value)
        {
            _slice.SetShort(_size, value);
            _size += SizeOf.Short;
        }

        public override void WriteUnsignedShort(ushort value)
        {
            _slice.SetUnsignedShort(_size, value);
            _size += SizeOf.Short;
        }

        public override void WriteInt(int value)
        {
            _slice.SetInt(_size, value);
            _size += SizeOf.Int;
        }

        public override void WriteUnsignedInt(uint value)
        {
            _slice.SetUnsignedInt(_size, value);
            _size += SizeOf.Int;
        }

        public override void WriteLong(long value)
        {
            _slice.SetLong(_size, value);
            _size += SizeOf.Long;
        }

        public override void WriteUnsignedLong(ulong value)
        {
            _slice.SetUnsignedLong(_size, value);
            _size += SizeOf.Long;
        }

        public override void WriteBytes(byte[] source, int sourceIndex, int length)
        {
            _slice.SetBytes(_size, source, sourceIndex, length);
            _size += length;
        }

        public override void WriteBytes(byte[] source)
        {
            WriteBytes(source, 0, source.Length);
        }

        public override void WriteBytes(Slice source)
        {
            WriteBytes(source, 0, source.Length);
        }

        public override void WriteBytes(SliceInput source, int length)
        {
            if (length > source.Available)
            {
                throw new IndexOutOfRangeException();
            }
            WriteBytes(source.ReadBytes(length));
        }

        public override void WriteBytes(Slice source, int sourceIndex, int length)
        {
            _slice.SetBytes(_size, source, sourceIndex, length);
			_size += length;
        }

        public override void WriteBytes(MemoryStream source)
        {
            var length = (int) source.Remaining();
            _slice.SetBytes(_size, source);
            _size += length;
        }

        public override int WriteBytes(Stream inputStream, int length)
        {
            var writtenBytes = _slice.SetBytes(_size, inputStream, length);
            if (writtenBytes > 0)
            {
                _size += writtenBytes;
            }
            return writtenBytes;
        }

        public override void WriteZero(uint length)
        {
            if (length == 0)
            {
                return;
            }
            var nLong = length >> 3;
            var nBytes = length & 7;
            for (var i = nLong; i > 0; i--)
            {
                WriteLong(0);
            }
            if (nBytes == 4)
            {
                WriteInt(0);
            }
            else if (nBytes < 4)
            {
                for (var i = nBytes; i > 0; i--)
                {
                    WriteByte(0);
                }
            }
            else
            {
                WriteInt(0);
                for (var i = nBytes - 4; i > 0; i--)
                {
                    WriteByte(0);
                }
            }
        }

        public override Slice Sliced()
        {
            return _slice.Sliced(0, _size);
        }

        public override MemoryStream ToMemoryStream()
        {
            return _slice.ToMemoryStream(0, _size);
        }

        public override void Flush()
        {
        }

        public override string ToString()
        {
            return $"{GetType().Name}(size={_size}, capacity={_slice.Length})";
        }

        public override string ToString(Encoding charset)
        {
            return _slice.ToString(0, _size, charset);
        }

        #region Unsupported methods

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}