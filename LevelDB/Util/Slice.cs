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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using LevelDB.Guava;
using LevelDB.Util.Extension;

namespace LevelDB.Util
{
    public sealed class Slice : IComparable<Slice>
    {
        private readonly byte[] _data;
        private readonly int _offset;

        private int _hash;

        /// <summary>
        /// Length of this slice.
        /// </summary>
        public int Length { get; }

        public Slice(int length)
        {
            _data = new byte[length];
            _offset = 0;
            Length = length;
        }

        public Slice(byte[] data)
        {
            Preconditions.CheckNotNull(data, "array is null");
            _data = data;
            _offset = 0;
            Length = data.Length;
        }

        public Slice(byte[] data, int offset, int length)
        {
            Preconditions.CheckNotNull(data, "array is null");
            _data = data;
            _offset = offset;
            Length = length;
        }

        /// <summary>
        /// Gets the array underlying this slice.
        /// </summary>
        /// <returns>Array</returns>
        public byte[] GetRawArray()
        {
            return _data;
        }

        /// <summary>
        /// Gets the offset of this slice in the underlying array.
        /// </summary>
        /// <returns>The offset of this slice</returns>
        public int GetRawOffset()
        {
            return _offset;
        }

        /// <summary>
        /// Gets a signed byte at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>An unsigned byte value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 1</code> is greater than <code>this.capacity</code></exception>
        public byte GetByte(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Byte, Length);
            index += _offset;
            return _data[index];
        }

        /// <summary>
        /// Gets an unsigned byte at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>A signed byte value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 1</code> is greater than <code>this.capacity</code></exception>
        public sbyte GetSignedByte(int index)
        {
            return unchecked ((sbyte) GetByte(index));
        }

        /// <summary>
        /// Gets a 16-bit signed short integer at the specified absolute <code>index</code> in this slice.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>A signed short value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 2</code> is greater than <code>this.capacity</code></exception>
        public short GetShort(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Short, Length);
            index += _offset;
            return BitConverterEx.ToShort(_data, index, Endian.Little);
        }

        /// <summary>
        /// Gets a 16-bit unsigned short integer at the specified absolute <code>index</code> in this slice.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>An unsigned short value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 2</code> is greater than <code>this.capacity</code></exception>
        public ushort GetUnsignedShort(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Short, Length);
            index += _offset;
            return BitConverterEx.ToUndignedShort(_data, index, Endian.Little);
        }

        /// <summary>
        /// Gets a 32-bit signed integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>A signed integer value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 4</code> is greater than <code>this.capacity</code></exception>
        public int GetInt(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Int, Length);
            index += _offset;
            return BitConverterEx.ToInt(_data, index, Endian.Little);
        }

        /// <summary>
        /// Gets a 32-bit unsigned integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>An unsigned integer value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 4</code> is greater than <code>this.capacity</code></exception>
        public uint GetUnsignedInt(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Int, Length);
            index += _offset;
            return BitConverterEx.ToUnsignedInt(_data, index, Endian.Little);
        }

        /// <summary>
        /// Gets a 64-bit signed long integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>A signed long value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 8</code> is greater than <code>this.capacity</code></exception>
        public long GetLong(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Long, Length);
            index += _offset;
            return BitConverterEx.ToLong(_data, index, Endian.Little);
        }

        /// <summary>
        /// Gets a 64-bit unsigned long integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <returns>An unsigned long value</returns>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 8</code> is greater than <code>this.capacity</code></exception>
        public ulong GetUnsignedLong(int index)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Long, Length);
            index += _offset;
            return BitConverterEx.ToUnsignedLong(_data, index, Endian.Little);
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="dst">The destination</param>
        /// <param name="dstIndex">The first index of the destination</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code>,
        /// if the specified <code>dstIndex</code> is less than <code>0</code>,
        /// if <code>index + length</code> is greater than
        /// <code>this.capacity</code>, or
        /// if <code>dstIndex + length</code> is greater than
        /// <code>dst.length</code></exception>
        public void GetBytes(int index, Slice dst, int dstIndex, int length)
        {
            GetBytes(index, dst._data, dstIndex, length);
        }

        /// <summary>
        ///  Transfers this buffer's data to the specified destination starting at the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="destination">The destination</param>
        /// <param name="destinationIndex">The first index of the destination</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code>,
        /// if the specified <code>dstIndex</code> is less than <code>0</code>,
        /// if <code>index + length</code> is greater than
        /// <code>this.capacity</code>, or
        /// if <code>dstIndex + length</code> is greater than
        /// <code>dst.length</code></exception>
        public void GetBytes(int index, byte[] destination, int destinationIndex, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            Preconditions.CheckPositionIndexes(destinationIndex, destinationIndex + length, destination.Length);
            index += _offset;
            Array.Copy(_data, index, destination, destinationIndex, length);
        }

        public byte[] GetBytes()
        {
            return GetBytes(0, Length);
        }

        public byte[] GetBytes(int index, int length)
        {
            index += _offset;
            var value = new byte[length];
            Array.Copy(_data, index, value, 0, length);
            return value;
        }

        /// <summary>
        /// Transfers this buffer's data to the specified destination starting at the specified absolute <code>index</code>
        /// until the destination's position reaches its limit.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="destination">The destination</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// if <code>index + dst.remaining()</code> is greater than
        /// <code>this.capacity</code></exception>
        public void GetBytes(int index, Stream destination)
        {
            Preconditions.CheckPositionIndex(index, Length);
            index += _offset;
            destination.Put(_data, index, Math.Min(Length, (int) destination.Remaining()));
        }

        /// <summary>
        /// Transfers this buffer's data to the specified stream starting at the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="outStream">The destination</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// if <code>index + dst.remaining()</code> is greater than
        /// <code>this.capacity</code></exception>
        public void GetBytes(int index, Stream outStream, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            index += _offset;
            outStream.Write(_data, index, length);
        }

        /// <summary>
        /// Sets the specified 16-bit signed short integer at the specified absolute <code>index</code> in this buffer.
        /// The 16 high-order bits of the specified value are ignored.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">A signed short value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 2</code> is greater than <code>this.capacity</code></exception>
        public void SetShort(int index, short value)
        {
            SetUnsignedShort(index, (ushort) value);
        }

        /// <summary>
        /// Sets the specified 16-bit unsigned short integer at the specified absolute <code>index</code> in this buffer.
        /// The 16 high-order bits of the specified value are ignored.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">An unsigned short value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 2</code> is greater than <code>this.capacity</code></exception>
        public void SetUnsignedShort(int index, ushort value)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Short, Length);
            index += _offset;
            _data[index] = (byte) value;
            _data[index + 1] = (byte) (value >> 8);
        }

        /// <summary>
        /// Sets the specified 32-bit signed integer at the specified absolute
        /// <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">A signed integer value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 4</code> is greater than <code>this.capacity</code></exception>
        public void SetInt(int index, int value)
        {
            SetUnsignedInt(index, (uint) value);
        }

        /// <summary>
        /// Sets the specified 32-bit unsigned integer at the specified absolute
        /// <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">An unsigned integer value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 4</code> is greater than <code>this.capacity</code></exception>
        public void SetUnsignedInt(int index, uint value)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Int, Length);
            index += _offset;
            _data[index] = (byte) value;
            _data[index + 1] = (byte) (value >> 8);
            _data[index + 2] = (byte) (value >> 16);
            _data[index + 3] = (byte) (value >> 24);
        }

        /// <summary>
        /// Sets the specified 64-bit signed long integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">A signed long value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 8</code> is greater than <code>this.capacity</code></exception>
        public void SetLong(int index, long value)
        {
            SetUnsignedLong(index, (ulong) value);
        }

        /// <summary>
        /// Sets the specified 64-bit unsigned long integer at the specified absolute <code>index</code> in this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">An unsigned long value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 8</code> is greater than <code>this.capacity</code></exception>
        public void SetUnsignedLong(int index, ulong value)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Long, Length);
            index += _offset;
            _data[index] = (byte) value;
            _data[index + 1] = (byte) (value >> 8);
            _data[index + 2] = (byte) (value >> 16);
            _data[index + 3] = (byte) (value >> 24);
            _data[index + 4] = (byte) (value >> 32);
            _data[index + 5] = (byte) (value >> 40);
            _data[index + 6] = (byte) (value >> 48);
            _data[index + 7] = (byte) (value >> 56);
        }

        /// <summary>
        /// Sets the specified byte at the specified absolute <code>index</code> in this buffer.
        /// The 24 high-order bits of the specified value are ignored.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">The value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 1</code> is greater than <code>this.capacity</code></exception>
        public void SetByte(int index, byte value)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Byte, Length);
            index += _offset;
            _data[index] = value;
        }

        /// <summary>
        /// Sets the specified signed byte at the specified absolute <code>index</code> in this buffer.
        /// The 24 high-order bits of the specified value are ignored.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="value">A signed byte value</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// <code>index + 1</code> is greater than <code>this.capacity</code></exception>
        public void SetSignedByte(int index, sbyte value)
        {
            Preconditions.CheckPositionIndexes(index, index + SizeOf.Byte, Length);
            index += _offset;
            _data[index] = (byte) value;
        }

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="src">The source Slice</param>
        /// <param name="srcIndex">The first index of the source</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code>,
        /// if the specified <code>srcIndex</code> is less than <code>0</code>,
        /// if <code>index + length</code> is greater than
        /// <code>this.capacity</code>, or
        /// if <code>srcIndex + length</code> is greater than
        /// <code>src.capacity</code></exception>
        public void SetBytes(int index, Slice src, int srcIndex, int length)
        {
            SetBytes(index, src._data, src._offset + srcIndex, length);
        }

        /// <summary>
        /// Transfers the specified source array's data to this buffer starting at
        /// the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="source">The source Slice</param>
        /// <param name="sourceIndex">The first index of the source</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code>,
        /// if the specified <code>srcIndex</code> is less than <code>0</code>,
        /// if <code>index + length</code> is greater than
        /// <code>this.capacity</code>, or
        /// if <code>srcIndex + length</code> is greater than <code>src.length</code></exception>
        public void SetBytes(int index, byte[] source, int sourceIndex, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            Preconditions.CheckPositionIndexes(sourceIndex, sourceIndex + length, source.Length);
            index += _offset;
            Array.Copy(source, sourceIndex, _data, index, length);
        }

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the specified
        /// absolute <code>index</code> until the source buffer's position reaches its limit.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="source">The source stream</param>
        /// <exception cref="IndexOutOfRangeException">If the specified <code>index</code> is less than <code>0</code> or
        /// if <code>index + src.remaining()</code> is greater than
        /// <code>this.capacity</code></exception>
        public void SetBytes(int index, MemoryStream source)
        {
            Preconditions.CheckPositionIndexes(index, (int) (index + source.Remaining()), Length);
            index += _offset;
            source.Read(_data, index, (int) source.Remaining());
        }

        /// <summary>
        /// Transfers the content of the specified source stream to this buffer starting at the specified absolute <code>index</code>.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="input">The source stream</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <returns>The actual number of bytes read in from the specified channel. <code>-1</code> if the specified channel is closed.</returns>
        /// <exception cref="IndexOutOfRangeException">if the specified <code>index</code> is less than <code>0</code> or
        /// if <code>index + length</code> is greater than <code>this.capacity</code></exception>
        public int SetBytes(int index, Stream input, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            index += _offset;
            var readBytes = 0;
            do
            {
                if (input.IsEof())
                {
                    if (readBytes == 0)
                    {
                        return -1;
                    }
                    break;
                }
                var localReadBytes = input.Read(_data, index, length);                
                readBytes += localReadBytes;
                index += localReadBytes;
                length -= localReadBytes;
            } while (length > 0);

            return readBytes;
        }

        public Slice CopySlice()
        {
            return CopySlice(0, Length);
        }

        /// <summary>
        /// Returns a copy of this buffer's sub-region. Modifying the content of the returned buffer or this buffer
        /// does not affect each other at all.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="length">The length</param>
        /// <returns>The copied <code>Slice</code></returns>
        public Slice CopySlice(int index, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            index += _offset;
            var copiedArray = new byte[length];
            Array.Copy(_data, index, copiedArray, 0, length);
            return new Slice(copiedArray);
        }

        public byte[] CopyBytes()
        {
            return CopyBytes(0, Length);
        }

        public byte[] CopyBytes(int index, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            index += _offset;
            var value = new byte[length];
            Array.Copy(_data, index, value, 0, length);
            return value;
        }

        /// <summary>
        /// Returns a slice of this buffer's readable bytes. Modifying the content of the returned buffer or
        /// this buffer affects each other's content while they maintain separate indexes and marks.
        /// </summary>
        /// <returns>The copy of Slice</returns>
        public Slice Sliced()
        {
            return Sliced(0, Length);
        }

        /// <summary>
        /// Returns a slice of this buffer's sub-region. Modifying the content of the returned buffer or
        /// this buffer affects each other's content while they maintain separate indexes and marks.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="length">The length</param>
        /// <returns>The sliced Slice</returns>
        public Slice Sliced(int index, int length)
        {
            if (index == 0 && length == Length)
            {
                return this;
            }
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            if (index >= 0 && length == 0)
            {
                return Slices.EmptySlice;
            }
            return new Slice(_data, _offset + index, length);
        }

        /// <summary>
        /// Creates an input stream over this slice.
        /// </summary>
        /// <returns></returns>
        public SliceInput Input()
        {
            return new SliceInput(this);
        }

        /// <summary>
        /// Creates an output stream over this slice.
        /// </summary>
        /// <returns></returns>
        public SliceOutput Output()
        {
            return new BasicSliceOutput(this);
        }

        /// <summary>
        /// Converts this buffer's readable bytes into the MemoryStream. The returned buffer shares the content with this buffer.
        /// </summary>
        /// <returns></returns>
        public MemoryStream ToMemoryStream()
        {
            return ToMemoryStream(0, Length);
        }

        /// <summary>
        /// Converts this buffer's sub-region into the MemoryStream.  The returned buffer shares the content with this buffer.
        /// </summary>
        /// <param name="index">The index</param>
        /// <param name="length">The length></param>
        /// <returns></returns>
        public MemoryStream ToMemoryStream(int index, int length)
        {
            Preconditions.CheckPositionIndexes(index, index + length, Length);
            index += _offset;
			return new MemoryStream(_data, index, length);
        }

        #region Override methods

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || GetType() != o.GetType())
            {
                return false;
            }

            var slice = (Slice) o;

            // do lengths match
            if (Length != slice.Length)
            {
                return false;
            }

            // if arrays have same base offset, some optimizations can be taken...
            if (_offset == slice._offset && _data == slice._data)
            {
                return true;
            }
            for (var i = 0; i < Length; i++)
            {
                if (_data[_offset + i] != slice._data[slice._offset + i])
                {
                    return false;
                }
            }
            return true;
        }

        [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
        public override int GetHashCode()
        {
            if (_hash != 0)
            {
                return _hash;
            }
            var result = Length;
            for (var i = _offset; i < _offset + Length; i++)
            {
                result = 31 * result + _data[i];
            }
            if (result == 0)
            {
                result = 1;
            }
            _hash = result;
            return _hash;
        }

        /// <summary>
        /// Compares the content of the specified buffer to the content of this
        /// buffer.  This comparison is performed byte by byte using an unsigned
        /// comparison.
        /// </summary>
        /// <param name="that"></param>
        /// <returns></returns>
        public int CompareTo(Slice that)
        {
            if (that == null)
            {
                return 0;
            }
            if (ReferenceEquals(this, that))
            {
                return 0;
            }
            if (_data == that._data && Length == that.Length && _offset == that._offset)
            {
                return 0;
            }

            var minLength = Math.Min(Length, that.Length);
            for (var i = 0; i < minLength; i++)
            {
                var thisByte = 0xFF & _data[_offset + i];
                var thatByte = 0xFF & that._data[that._offset + i];
                if (thisByte != thatByte)
                {
                    return thisByte - thatByte;
                }
            }
            return Length - that.Length;
        }

        public string ToString(Encoding charset)
        {
            return ToString(0, Length, charset);
        }

        public string ToString(int index, int length, Encoding charset)
        {
			return length == 0 ? "" : Slices.DecodeString(ToMemoryStream(index, length), charset);
        }

        public override string ToString()
        {
            return GetType().Name + '(' + "length=" + Length + ')';
        }

        #endregion
    }
}