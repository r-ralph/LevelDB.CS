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
    public sealed class SliceInput : BaseInputStream, IDataInput
    {
        private readonly Slice _slice;

        private int _position;

        public override long Position
        {
            get => _position;
            set
            {
                if (value < 0 || value > _slice.Length)
                {
                    throw new IndexOutOfRangeException();
                }
                _position = (int) value;
            }
        }

        public override bool CanRead => Available > 0;

        public override int Available => _slice.Length - _position;

        public override long Length => _slice.Length;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        public SliceInput(Slice slice)
        {
            _slice = slice;
        }

        public bool ReadBoolean()
        {
            return ReadByte() != 0;
        }

        /// <summary>
        /// Gets an unsigned byte at the current <code>position</code> and increases the <code>position</code> by <code>1</code> in this buffer.
        /// </summary>
        /// <returns>An unsigned byte value</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>this.available()</code> is less than <code>1</code></exception>
        public byte ReadByteAlt()
        {
            if (_position == _slice.Length)
            {
                throw new IndexOutOfRangeException();
            }
            return _slice.GetByte(_position++);
        }

        public override int ReadByte()
        {
            return ReadByteAlt();
        }

        /// <summary>
        /// Gets a signed byte at the current <code>position</code> and increases the <code>position</code> by <code>1</code> in this buffer.
        /// </summary>
        /// <returns>A signed byte value</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>this.available()</code> is less than <code>1</code></exception>
        public sbyte ReadSignedByte()
        {
            if (_position == _slice.Length)
            {
                throw new IndexOutOfRangeException();
            }
            return _slice.GetSignedByte(_position++);
        }

        /// <summary>
        /// Gets a 16-bit signed short integer at the current <code>position</code> and increases the <code>position</code> by <code>2</code> in this buffer.
        /// </summary>
        /// <returns>A signed short value</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>this.available()</code> is less than <code>2</code></exception>
        public short ReadShort()
        {
            var v = _slice.GetShort(_position);
            _position += SizeOf.Short;
            return v;
        }

        /// <summary>
        /// Gets a 16-bit unsigned short integer at the current <code>position</code> and increases the <code>position</code> by <code>2</code> in this buffer.
        /// </summary>
        /// <returns>An unsigned short value</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>this.available()</code> is less than <code>2</code></exception>
        public ushort ReadUnsignedShort()
        {
            var v = _slice.GetUnsignedShort(_position);
            _position += SizeOf.Short;
            return v;
        }

        /// <summary>
        /// Gets a 32-bit signed integer at the current <code>position</code> and increases the <code>position</code> by <code>4</code> in this buffer.
        /// </summary>
        /// <returns>A signed integer value</returns>
        /// <exception cref="IndexOutOfRangeException">If <code>this.available()</code> is less than <code>4</code></exception>
        public int ReadInt()
        {
            var v = _slice.GetInt(_position);
            _position += SizeOf.Int;
            return v;
        }

        /// <summary>
        /// Gets a 32-bit unsigned integer at the current <code>position</code> and increases the <code>position</code> by <code>4</code> in this buffer.
        /// </summary>
        /// <returns>An unsigned integer value</returns>
        /// <exception cref="IndexOutOfRangeException">If <code>this.available()</code> is less than <code>4</code></exception>
        public uint ReadUnsignedInt()
        {
            var v = _slice.GetUnsignedInt(_position);
            _position += SizeOf.Int;
            return v;
        }

        /// <summary>
        /// Gets a 64-bit signed integer at the current <code>position</code> and increases the <code>position</code> by <code>8</code> in this buffer.
        /// </summary>
        /// <returns>A signed long value</returns>
        /// <exception cref="IndexOutOfRangeException">If <code>this.available()</code> is less than <code>8</code></exception>
        public long ReadLong()
        {
            var v = _slice.GetLong(_position);
            _position += SizeOf.Long;
            return v;
        }

        /// <summary>
        /// Gets a 64-bit unsigned integer at the current <code>position</code> and increases the <code>position</code> by <code>8</code> in this buffer.
        /// </summary>
        /// <returns>An unsigned long value</returns>
        /// <exception cref="IndexOutOfRangeException">If <code>this.available()</code> is less than <code>8</code></exception>
        public ulong ReadUnsignedLong()
        {
            var v = _slice.GetUnsignedLong(_position);
            _position += SizeOf.Long;
            return v;
        }

        public byte[] ReadByteArray(int length)
        {
            var value = _slice.CopyBytes(_position, length);
            _position += length;
            return value;
        }

        /// <summary>
        /// Transfers this buffer's data to a newly created buffer starting at the current <code>position</code> and
        /// increases the <code>position</code> by the number of the transferred bytes (= <code>length</code>).
        /// The returned buffer's <code>position</code> and <code>writerIndex</code> are <code>0</code> and <code>length</code> respectively.
        /// </summary>
        /// <param name="length">The number of bytes to transfer</param>
        /// <returns>The newly created buffer which contains the transferred bytes</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>length</code> is greater than <code>this.Available</code></exception>
        public Slice ReadBytes(int length)
        {
            if (length == 0)
            {
                return Slices.EmptySlice;
            }
            var value = _slice.Sliced(_position, length);
            _position += length;
            return value;
        }

        /// <summary>
        /// Returns a new slice of this buffer's sub-region starting at the current <code>position</code> and increases
        /// the <code>position</code> by the size of the new slice (= <code>length</code>).
        /// </summary>
        /// <param name="length">The size of the new slice</param>
        /// <returns>The newly created slice</returns>
        /// <exception cref="IndexOutOfRangeException">if <code>length</code> is greater than <code>this.Available</code></exception>
        public Slice ReadSlice(int length)
        {
            var newSlice = _slice.Sliced(_position, length);
            _position += length;
            return newSlice;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> and
        /// increases the <code>position</code> by the number of the transferred bytes (= <code>destination.Length</code>).
        /// </summary>
        /// <param name="destination"></param>
        /// <exception cref="IndexOutOfRangeException">If <code>dst.length</code> is greater than <code>this.Available</code></exception>
        public void ReadFully(byte[] destination)
        {
            ReadBytes(destination);
        }

        public void ReadBytes(byte[] destination)
        {
            ReadBytes(destination, 0, destination.Length);
        }

        public void ReadFully(byte[] destination, int offset, int length)
        {
            ReadBytes(destination, offset, length);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            var prevPos = _position;
            ReadBytes(buffer, offset, count);
            return _position - prevPos;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> and
        /// increases the <code>position</code> by the number of the transferred bytes (= <code>length</code>).
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="destinationIndex">The first index of the destination</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">if the specified <code>destinationIndex</code> is less than <code>0</code>,
        /// if <code>length</code> is greater than <code>this.available()</code>, or
        /// if <code>destinationIndex + length</code> is greater than <code>destination.length</code></exception>
        public void ReadBytes(byte[] destination, int destinationIndex, int length)
        {
            _slice.GetBytes(_position, destination, destinationIndex, length);
            _position += length;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> until
        /// the destination becomes non-writable, and increases the <code>position</code> by the number of the transferred bytes.
        /// This method is basically same with <code cref="ReadBytes(Slice, int, int)" />, except that this method
        /// increases the <code>writerIndex</code> of the destination by the number of the transferred bytes while
        /// <code cref="ReadBytes(Slice, int, int)" /> does not.
        /// </summary>
        /// <param name="destination"></param>
        /// <exception cref="IndexOutOfRangeException">If <code>destination.writableBytes</code> is greater than <code>this.Available</code></exception>
        public void ReadBytes(Slice destination)
        {
            ReadBytes(destination, destination.Length);
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> and
        /// increases the <code>position</code> by the number of the transferred bytes (= <code>length</code>).  This method is
        /// basically same with <code cref="ReadBytes(Slice, int, int)" />, except that this method increases the
        /// <code>writerIndex</code> of the destination by the number of the transferred bytes (= <code>length</code>) while
        /// <code cref="ReadBytes(Slice, int, int)" /> does not.
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="length"></param>
        /// <exception cref="IndexOutOfRangeException">If <code>length</code> is greater than <code>this.Available</code> or
        /// if <code>Length</code> is greater than <code>destination.writableBytes</code></exception>
        public void ReadBytes(Slice destination, int length)
        {
            if (length > destination.Length)
            {
                throw new IndexOutOfRangeException();
            }
            ReadBytes(destination, destination.Length, length);
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> and
        /// increases the <code>position</code> by the number of the transferred bytes (= <code>length</code>).
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="destinationIndex">The first index of the destination</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>destinationIndex</code> is less than <code>0</code>,
        /// if <code>length</code> is greater than <code>this.available()</code>, or
        /// if <code>destinationIndex + length</code> is greater than
        /// <code>destination.capacity</code></exception>
        public void ReadBytes(Slice destination, int destinationIndex, int length)
        {
            _slice.GetBytes(_position, destination, destinationIndex, length);
            _position += length;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the current <code>position</code> until
        /// the destination's position reaches its limit, and increases the <code>position</code> by the number of the
        /// transferred bytes.
        /// </summary>
        /// <param name="destination"></param>
        /// <exception cref="IndexOutOfRangeException">If <code>destination.Remaining()</code> is greater than <code>this.Available</code></exception>
        public void ReadBytes(MemoryStream destination)
        {
            var length = destination.Remaining();
            _slice.GetBytes(_position, destination);
            _position += (int) length;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified stream starting at the current <code>position</code>.
        /// </summary>
        /// <param name="outStream"></param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">if <code>length</code> is greater than <code>this.Available</code></exception>
        public void ReadBytes(Stream outStream, int length)
        {
            _slice.GetBytes(_position, outStream, length);
            _position += length;
        }

        /// <summary>
        /// Returns a slice of this buffer's readable bytes. Modifying the content of the returned buffer or this buffer
        /// affects each other's content while they maintain separate indexes and marks. This method is identical to
        ///  <code>buf.sliced(buf.position(), buf.Available)</code>. This method does not modify <code>position</code> or
        /// <code>writerIndex</code> of this buffer.
        /// </summary>
        /// <returns></returns>
        public Slice Sliced()
        {
            return _slice.Sliced(_position, Available);
        }

        /// <summary>
        /// Converts this buffer's readable bytes into a MemoryBuffer. The returned stream might or might not share
        /// the content with this buffer, while they have separate indexes and marks. This method is identical to
        /// <code>buf.toMemoryStream(buf.position(), buf.Available)</code>. This method does not modify <code>position</code> or
        /// <code>writerIndex</code> of this buffer.
        /// </summary>
        /// <returns></returns>
        public MemoryStream ToMemoryStream()
        {
            return _slice.ToMemoryStream(_position, Available);
        }

        /// <summary>
        /// Decodes this buffer's readable bytes into a string with the specified character set name. This method is
        /// identical to <code>buf.toString(buf.position(), buf.Available, charsetName)</code>. This method does not modify
        /// <code>position</code> or <code>writerIndex</code> of this buffer.
        /// </summary>
        /// <param name="charset"></param>
        /// <returns></returns>
        public string ToString(Encoding charset)
        {
            return _slice.ToString(_position, Available, charset);
        }

        public override string ToString()
        {
            return $"{GetType().Name}(ridx={_position}, cap={_slice.Length})";
        }

        public override void Flush()
        {
        }

        #region Unsupported operations

        public int SkipBytes(int n)
        {
            throw new NotSupportedException();
        }

        public char ReadChar()
        {
            throw new NotSupportedException();
        }

        public float ReadFloat()
        {
            throw new NotSupportedException();
        }

        public double ReadDouble()
        {
            throw new NotSupportedException();
        }

        public string ReadLine()
        {
            throw new NotSupportedException();
        }

        public string ReadUTF()
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}