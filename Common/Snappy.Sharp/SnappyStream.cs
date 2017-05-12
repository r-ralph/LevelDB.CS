using System;
using System.IO;
using System.IO.Compression;

namespace Snappy.Sharp
{
    // Modeled after System.IO.Compression.DeflateStream in the framework
    public class SnappyStream : Stream
    {
        private const int BlockLog = 15;
        private const int BlockSize = 1 << BlockLog;

        private readonly CompressionMode _compressionMode;
        private readonly bool _leaveStreamOpen;
        private readonly bool _writeChecksums;

        private static readonly byte[] StreamHeader =
            {(byte) 's', (byte) 'N', (byte) 'a', (byte) 'P', (byte) 'p', (byte) 'Y'};

        private const byte StreamIdentifier = 0xff;
        private const byte CompressedType = 0x00;
        private const byte UncompressedType = 0x01;

        // allocate a 64kB buffer for the (de)compressor to use
        private readonly byte[] _internalBuffer = new byte[1 << (BlockLog + 1)];

        private int _internalBufferIndex;
        private int _internalBufferLength;

        private readonly SnappyCompressor _compressor;
        private readonly SnappyDecompressor _decompressor;

        /// <summary>
        /// Provides access to the underlying (compressed) <see cref="T:System.IO.Stream"/>.
        /// </summary>
        public Stream BaseStream { get; private set; }

        public override bool CanRead => BaseStream != null && _compressionMode == CompressionMode.Decompress &&
                                        BaseStream.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => BaseStream != null && _compressionMode == CompressionMode.Compress &&
                                         BaseStream.CanWrite;

        /// <summary>
        /// This property is not supported and always throws a <see cref="T:System.NotSupportedException" />.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">This property is not supported on this stream.</exception>
        public override long Length => throw new NotSupportedException();

        /// <summary>
        /// This property is not supported and always throws a <see cref="T:System.NotSupportedException" />.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">This property is not supported on this stream.</exception>
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SnappyStream"/> class.
        /// </summary>
        /// <param name="s">The stream.</param>
        /// <param name="mode">The compression mode.</param>
        public SnappyStream(Stream s, CompressionMode mode) : this(s, mode, false, false)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SnappyStream"/> class.
        /// </summary>
        /// <param name="s">The stream.</param>
        /// <param name="mode">The compression mode.</param>
        /// <param name="leaveOpen">If set to <c>true</c> leaves the stream open when complete.</param>
        /// <param name="checksum"><c>true</c> if checksums should be written to the stream </param>
        public SnappyStream(Stream s, CompressionMode mode, bool leaveOpen, bool checksum)
        {
            BaseStream = s;
            _compressionMode = mode;
            _leaveStreamOpen = leaveOpen;
            _writeChecksums = checksum;

            if (_compressionMode == CompressionMode.Decompress)
            {
                if (!BaseStream.CanRead)
                    throw new InvalidOperationException("Trying to decompress and underlying stream not readable.");

                _decompressor = new SnappyDecompressor();

                CheckStreamIdentifier();
                CheckStreamHeader();
            }
            if (_compressionMode == CompressionMode.Compress)
            {
                if (!BaseStream.CanWrite)
                    throw new InvalidOperationException("Trying to compress and underlying stream is not writable.");

                _compressor = new SnappyCompressor();

                BaseStream.WriteByte(StreamIdentifier);
                BaseStream.Write(StreamHeader, 0, StreamHeader.Length);
            }
        }


        public override void Flush()
        {
            BaseStream?.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_compressionMode != CompressionMode.Decompress || _decompressor == null)
                throw new InvalidOperationException("Cannot read if not set to decompression mode.");

            var readCount = 0;
            var firstByte = BaseStream.ReadByte();
            // first byte can indicate stream header, we just read it and move on.
            switch (firstByte)
            {
                case StreamIdentifier:
                    CheckStreamHeader();
                    break;
                case UncompressedType:
                {
                    var length = GetChunkUncompressedLength();
                    readCount = ProcessRemainingInternalBuffer(buffer, offset, count);
                    if (readCount != count)
                    {
                        BaseStream.Read(_internalBuffer, 0, length);
                        Array.Copy(_internalBuffer, 0, buffer, offset, count - readCount);
                        _internalBufferIndex = count - readCount;
                        _internalBufferLength = length;
                    }
                    break;
                }
                case CompressedType:
                {
                    var length = GetChunkUncompressedLength();
                    count = ProcessRemainingInternalBuffer(buffer, offset, count);

                    // we at most have 64kb in the buffer to read
                    var tempBuffer = new byte[1 << (BlockLog + 1)];
                    BaseStream.Read(tempBuffer, 0, tempBuffer.Length);

                    _decompressor.Decompress(tempBuffer, 0, tempBuffer.Length, _internalBuffer, 0, length);

                    Array.Copy(_internalBuffer, 0, buffer, offset, count);
                    _internalBufferIndex = count;
                    _internalBufferLength = length;
                    break;
                }
                default:
                    if (firstByte > 0x2 && firstByte < 0x7f)
                    {
                        throw new InvalidOperationException("Found unskippable chunk type that cannot be undertood.");
                    }
                    else
                    {
                        // getting the length and skipping the data.
                        var length = GetChunkUncompressedLength();
                        BaseStream.Seek(length, SeekOrigin.Current);
                        readCount += length;
                    }
                    break;
            }
            return readCount;
        }

        private int ProcessRemainingInternalBuffer(byte[] buffer, int offset, int count)
        {
            if (_internalBufferLength - _internalBufferIndex > count)
            {
                Array.Copy(_internalBuffer, _internalBufferIndex, buffer, offset, count);
                _internalBufferIndex += count;
            }
            else if (_internalBufferLength > 0)
            {
                Array.Copy(_internalBuffer, _internalBufferIndex, buffer, offset,
                    _internalBufferLength - _internalBufferIndex);
                count -= _internalBufferLength - _internalBufferIndex;
            }
            return count;
        }

        private int GetChunkUncompressedLength()
        {
            var len1 = BaseStream.ReadByte();
            var len2 = BaseStream.ReadByte();
            var length = (len1 << 8) | len2;
            if (length > BlockSize)
                throw new InvalidOperationException("Chunk length is too big.");
            return length;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (_compressionMode != CompressionMode.Compress || _compressor == null)
                throw new InvalidOperationException("Cannot write if not set to compression mode.");

            if (buffer.Length < count)
                throw new InvalidOperationException();

            for (var i = 0; i < count; i += BlockSize)
            {
                BaseStream.WriteByte(CompressedType);
                _compressor.WriteUncomressedLength(buffer, 1, count);
                var compressedLength = _compressor.CompressInternal(buffer, offset, count, _internalBuffer, 2);
                BaseStream.Write(_internalBuffer, 0, compressedLength + 3);
            }
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                if (!disposing || BaseStream == null) return;
                Flush();
                if (_compressionMode == CompressionMode.Compress && BaseStream != null)
                {
                    // Make sure all data written
                }
            }
            finally
            {
                try
                {
                    if (disposing && !_leaveStreamOpen)
                    {
                        BaseStream?.Dispose();
                    }
                }
                finally
                {
                    BaseStream = null;
                    base.Dispose(disposing);
                }
            }
        }

        private void CheckStreamHeader()
        {
            var heading = new byte[StreamHeader.Length];
            BaseStream.Read(heading, 0, heading.Length);
            for (var i = 1; i < heading.Length; i++)
            {
                if (heading[i] != StreamHeader[i])
                    throw new InvalidDataException("Stream does not start with required header");
            }
        }

        private void CheckStreamIdentifier()
        {
            var firstByte = BaseStream.ReadByte();
            if (firstByte == -1)
                throw new InvalidOperationException("Found EOF when trying to read header.");
            if (firstByte != StreamIdentifier)
                throw new InvalidOperationException("Invalid stream identifier found.");
        }

        #region NotSupported methods

        /// <summary>
        /// This operation is not supported and always throws a <see cref="T:System.NotSupportedException" />.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">This operation is not supported on this stream.</exception>
        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// This operation is not supported and always throws a <see cref="T:System.NotSupportedException" />.
        /// </summary>
        /// <exception cref="T:System.NotSupportedException">This operation is not supported on this stream.</exception>
        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}