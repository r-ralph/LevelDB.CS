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

namespace LevelDB.Util
{
    public abstract class SliceOutput : Stream, IDataOutput
    {
        /// <summary>
        /// Resets this stream to the initial position.
        /// </summary>
        public abstract void Reset();

        /// <summary>
        /// Returns the {@code writerIndex} of this buffer.
        /// </summary>
        /// <returns></returns>
        public abstract int Size();

        /// <summary>
        /// Returns the number of writable bytes which is equal to
        /// {@code (this.capacity - this.writerIndex)}.
        /// </summary>
        /// <returns></returns>
        public abstract int WritableBytes();

        public void WriteBoolean(bool value)
        {
            WriteByte((byte) (value ? 1 : 0));
        }

        public abstract override void WriteByte(byte value);

        public abstract void WriteSignedByte(sbyte value);

        /// <summary>
        /// Sets the specified 16-bit signed short integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 2} in this buffer.
        /// </summary>
        /// <param name="value">A signed short value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 2}</exception>
        public abstract void WriteShort(short value);

        /// <summary>
        /// Sets the specified 16-bit unsigned short integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 2} in this buffer.
        /// </summary>
        /// <param name="value">An unsigned short value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 2}</exception>
        public abstract void WriteUnsignedShort(ushort value);

        /// <summary>
        /// Sets the specified 32-bit signed integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 4} in this buffer.
        /// </summary>
        /// <param name="value">A signed integer value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 4}</exception>
        public abstract void WriteInt(int value);

        /// <summary>
        /// Sets the specified 32-bit unsigned integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 4} in this buffer.
        /// </summary>
        /// <param name="value">An unsigned integer value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 4}</exception>
        public abstract void WriteUnsignedInt(uint value);

        /// <summary>
        /// Sets the specified 64-bit signed long integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 8} in this buffer.
        /// </summary>
        /// <param name="value">A signed long value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 8}</exception>
        public abstract void WriteLong(long value);

        /// <summary>
        /// Sets the specified 64-bit unsigned long integer at the current {@code writerIndex} and increases the
        /// {@code writerIndex} by {@code 8} in this buffer.
        /// </summary>
        /// <param name="value">An unsigned long value</param>
        /// <exception cref="IndexOutOfRangeException">If {@code this.WritableBytes} is less than {@code 8}</exception>
        public abstract void WriteUnsignedLong(ulong value);

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the current {@code writerIndex}
        /// until the source buffer becomes unreadable, and increases the {@code writerIndex} by the number of
        /// the transferred bytes. This method is basically same with <code cref="WriteBytes(Slice, int, int)" />,
        /// except that this method increases the {@code readerIndex} of the source buffer by the number of
        /// the transferred bytes while <code cref="WriteBytes(Slice, int, int)" /> does not.
        /// </summary>
        /// <param name="source"></param>
        /// <exception cref="IndexOutOfRangeException">if {@code source.readableBytes} is greater than
        /// {@code this.WritableBytes}</exception>
        public abstract void WriteBytes(Slice source);

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the current {@code writerIndex}
        /// and increases the {@code writerIndex} by the number of the transferred bytes (= {@code length}).
        /// This method is basically same with <code cref="WriteBytes(Slice, int, int)" />, except that this method
        /// increases the {@code readerIndex} of the source buffer by the number of the transferred bytes
        /// (= {@code length}) while {@link #writeBytes(Slice, int, int)} does not.
        /// </summary>
        /// <param name="source"></param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">if {@code length} is greater than {@code this.WritableBytes} or
        /// if {@code length} is greater then {@code source.ReadableBytes}</exception>
        public abstract void WriteBytes(SliceInput source, int length);

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the current {@code writerIndex}
        /// and increases the {@code writerIndex} by the number of the transferred bytes (= {@code length}).
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceIndex">The first index of the source</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">IndexOutOfBoundsException if the specified {@code sourceIndex}
        /// is less than {@code 0},
        /// if {@code sourceIndex + length} is greater than
        /// {@code source.capacity}, or
        /// if {@code length} is greater than {@code this.writableBytes}</exception>
        public abstract void WriteBytes(Slice source, int sourceIndex, int length);

        public void Write(byte[] source)
        {
            WriteBytes(source);
        }

        /// <summary>
        /// Transfers the specified source array's data to this buffer starting at the current {@code writerIndex} and
        /// increases the {@code writerIndex} by the number of the transferred bytes (= {@code source.length}).
        /// </summary>
        /// <param name="source"></param>
        /// <exception cref="IndexOutOfRangeException">If {@code source.length} is greater than {@code this.WritableBytes}</exception>
        public abstract void WriteBytes(byte[] source);

        public override void Write(byte[] source, int sourceIndex, int length)
        {
            WriteBytes(source, sourceIndex, length);
        }

        /// <summary>
        /// Transfers the specified source array's data to this buffer starting at the current {@code writerIndex} and
        /// increases the {@code writerIndex} by the number of the transferred bytes (= {@code length}).
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceIndex">The first index of the source</param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <exception cref="IndexOutOfRangeException">if the specified {@code sourceIndex} is less than {@code 0},
        /// if {@code sourceIndex + length} is greater than
        /// {@code source.length}, or
        /// if {@code length} is greater than {@code this.writableBytes}</exception>
        public abstract void WriteBytes(byte[] source, int sourceIndex, int length);

        /// <summary>
        /// Transfers the specified source buffer's data to this buffer starting at the current {@code writerIndex}
        /// until the source buffer's position reaches its limit, and increases the {@code writerIndex} by the
        /// number of the transferred bytes.
        /// </summary>
        /// <param name="source"></param>
        /// <exception cref="IndexOutOfRangeException">If {@code source.remaining()} is greater than
        /// {@code this.writableBytes}</exception>
        public abstract void WriteBytes(MemoryStream source);

        /// <summary>
        /// Transfers the content of the specified stream to this buffer starting at the current {@code writerIndex}
        /// and increases the {@code writerIndex} by the number of the transferred bytes.
        /// </summary>
        /// <param name="inputStream"></param>
        /// <param name="length">The number of bytes to transfer</param>
        /// <returns>The actual number of bytes read in from the specified stream</returns>
        /// <exception cref="IndexOutOfRangeException">If {@code length} is greater than {@code this.writableBytes}</exception>
        /// <exception cref="IOException">If the specified channel threw an exception during I/O</exception>
        public abstract int WriteBytes(Stream inputStream, int length);

        /// <summary>
        /// Fills this buffer with <tt>NUL (0x00)</tt> starting at the current {@code writerIndex} and increases
        /// the {@code writerIndex} by the specified {@code length}.
        /// </summary>
        /// <param name="length">The number of <tt>NUL</tt>s to write to the buffer</param>
        /// <exception cref="IndexOutOfRangeException">If {@code length} is greater than {@code this.writableBytes}</exception>
        public abstract void WriteZero(uint length);

        /// <summary>
        /// Returns a slice of this buffer's readable bytes. Modifying the content of the returned buffer or this
        /// buffer affects each other's content while they maintain separate indexes and marks. This method is
        /// identical to {@code buf.slice(buf.readerIndex(), buf.ReadableBytes)}. This method does not modify
        /// {@code readerIndex} or {@code writerIndex} of this buffer.
        /// </summary>
        /// <returns></returns>
        public abstract Slice Sliced();

        /// <summary>
        /// Converts this buffer's readable bytes into a MemoryBuffer. The returned buffer might or might not share
        /// the content with this buffer, while they have separate indexes and marks. This method is identical to
        /// {@code buf.toByteBuffer(buf.readerIndex(), buf.ReadableBytes)}. This method does not modify
        /// {@code readerIndex} or {@code writerIndex} of this buffer.
        /// </summary>
        /// <returns></returns>
        public abstract MemoryStream ToMemoryStream();

        /// <summary>
        /// Decodes this buffer's readable bytes into a string with the specified character set name.This method is
        /// identical to {@code buf.toString(buf.readerIndex(), buf.readableBytes(), charsetName)}. This method does
        /// not modify {@code readerIndex} or {@code writerIndex} of this buffer.
        /// </summary>
        /// <param name="charset"></param>
        /// <returns></returns>
        public abstract string ToString(Encoding charset);

        #region Unsupported methods

        public void WriteChar(char v)
        {
            throw new NotSupportedException();
        }

        public void WriteFloat(float v)
        {
            throw new NotSupportedException();
        }

        public void WriteDouble(double v)
        {
            throw new NotSupportedException();
        }

        public void WriteLine(string v)
        {
            throw new NotSupportedException();
        }

        public void WriteUTF(string v)
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}