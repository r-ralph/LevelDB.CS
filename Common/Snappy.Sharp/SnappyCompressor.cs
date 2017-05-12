﻿using System;
using System.Diagnostics;

namespace Snappy.Sharp
{
    public class SnappyCompressor
    {
        private const int BlockLog = 15;
        private const int BlockSize = 1 << BlockLog;

        private const int InputMarginBytes = 15;

        private const int MaxHashTableBits = 14;
        private const int MaxHashTableSize = 1 << MaxHashTableBits;

        private readonly Func<byte[], int, int, int, int> _findMatchLength;

        public SnappyCompressor() : this(Utilities.NativeIntPtrSize())
        {
        }

        public SnappyCompressor(int intPtrBytes)
        {
            switch (intPtrBytes)
            {
                case 4:
                    Debug.WriteLine("Using 32-bit optimized FindMatchLength");
                    _findMatchLength = FindMatchLength32;
                    break;
                case 8:
                    Debug.WriteLine("Using 64-bit optimized FindMatchLength");
                    _findMatchLength = FindMatchLength64;
                    break;
                default:
                    Debug.WriteLine("Using unoptimized FindMatchLength");
                    _findMatchLength = FindMatchLengthBasic;
                    break;
            }
        }

        public int MaxCompressedLength(int sourceLength)
        {
            // So says the code from Google.
            return 32 + sourceLength + sourceLength / 6;
        }

        public int Compress(byte[] uncompressed, int uncompressedOffset, int uncompressedLength, byte[] compressed)
        {
            return Compress(uncompressed, uncompressedOffset, uncompressedLength, compressed, 0);
        }

        public int Compress(byte[] uncompressed, int uncompressedOffset, int uncompressedLength, byte[] compressed,
            int compressedOffset)
        {
            var compressedIndex = WriteUncomressedLength(compressed, compressedOffset, uncompressedLength);
            var headLength = compressedIndex - compressedOffset;
            return headLength + CompressInternal(uncompressed, uncompressedOffset, uncompressedLength, compressed,
                       compressedIndex);
        }

        internal int CompressInternal(byte[] uncompressed, int uncompressedOffset, int uncompressedLength,
            byte[] compressed, int compressedOffset)
        {
            // first time through set to offset.
            var compressedIndex = compressedOffset;
            var hashTable = GetHashTable(uncompressedLength);

            for (var read = 0; read < uncompressedLength; read += BlockSize)
            {
                // Get encoding table for compression
                Array.Clear(hashTable, 0, hashTable.Length);

                compressedIndex = CompressFragment(
                    uncompressed,
                    uncompressedOffset + read,
                    Math.Min(uncompressedLength - read, BlockSize),
                    compressed,
                    compressedIndex,
                    hashTable);
            }
            return compressedIndex - compressedOffset;
        }

        internal int CompressFragment(byte[] input, int inputOffset, int inputSize, byte[] output, int outputIndex,
            short[] hashTable)
        {
            // "ip" is the input pointer, and "op" is the output pointer.
            var inputIndex = inputOffset;
            Debug.Assert(inputSize <= BlockSize);
            Debug.Assert((hashTable.Length & (hashTable.Length - 1)) == 0, "hashTable size must be a power of 2");
            var shift = (int) (32 - Utilities.Log2Floor((uint) hashTable.Length));
            //DCHECK_EQ(static_cast<int>(kuint32max >> shift), table_size - 1);
            var inputEnd = inputOffset + inputSize;
            var baseInputIndex = inputIndex;
            // Bytes in [next_emit, ip) will be emitted as literal bytes.  Or
            // [next_emit, ip_end) after the main loop.
            var nextEmitIndex = inputIndex;

            if (inputSize >= InputMarginBytes)
            {
                var ipLimit = inputOffset + inputSize - InputMarginBytes;

                var currentIndexBytes = Utilities.GetFourBytes(input, ++inputIndex);
                for (var nextHash = Hash(currentIndexBytes, shift);;)
                {
                    Debug.Assert(nextEmitIndex < inputIndex);
                    // The body of this loop calls EmitLiteral once and then EmitCopy one or
                    // more times.  (The exception is that when we're close to exhausting
                    // the input we goto emit_remainder.)
                    //
                    // In the first iteration of this loop we're just starting, so
                    // there's nothing to copy, so calling EmitLiteral once is
                    // necessary.  And we only start a new iteration when the
                    // current iteration has determined that a call to EmitLiteral will
                    // precede the next call to EmitCopy (if any).
                    //
                    // Step 1: Scan forward in the input looking for a 4-byte-long match.
                    // If we get close to exhausting the input then goto emit_remainder.
                    //
                    // Heuristic match skipping: If 32 bytes are scanned with no matches
                    // found, start looking only at every other byte. If 32 more bytes are
                    // scanned, look at every third byte, etc.. When a match is found,
                    // immediately go back to looking at every byte. This is a small loss
                    // (~5% performance, ~0.1% density) for compressible data due to more
                    // bookkeeping, but for non-compressible data (such as JPEG) it's a huge
                    // win since the compressor quickly "realizes" the data is incompressible
                    // and doesn't bother looking for matches everywhere.
                    //
                    // The "skip" variable keeps track of how many bytes there are since the
                    // last match; dividing it by 32 (ie. right-shifting by five) gives the
                    // number of bytes to move ahead for each iteration.
                    uint skip = 32;

                    var nextIp = inputIndex;
                    int candidate;
                    do
                    {
                        inputIndex = nextIp;
                        var hash = nextHash;
                        Debug.Assert(hash == Hash(Utilities.GetFourBytes(input, inputIndex), shift));
                        nextIp = (int) (inputIndex + (skip++ >> 5));
                        if (nextIp > ipLimit)
                        {
                            goto emit_remainder;
                        }
                        currentIndexBytes = Utilities.GetFourBytes(input, nextIp);
                        nextHash = Hash(currentIndexBytes, shift);
                        candidate = baseInputIndex + hashTable[hash];
                        Debug.Assert(candidate >= baseInputIndex);
                        Debug.Assert(candidate < inputIndex);

                        hashTable[hash] = (short) (inputIndex - baseInputIndex);
                    } while (Utilities.GetFourBytes(input, inputIndex) != Utilities.GetFourBytes(input, candidate));

                    // Step 2: A 4-byte match has been found.  We'll later see if more
                    // than 4 bytes match.  But, prior to the match, input
                    // bytes [next_emit, ip) are unmatched.  Emit them as "literal bytes."
                    Debug.Assert(nextEmitIndex + 16 < inputEnd);
                    outputIndex = EmitLiteral(output, outputIndex, input, nextEmitIndex, inputIndex - nextEmitIndex,
                        true);

                    // Step 3: Call EmitCopy, and then see if another EmitCopy could
                    // be our next move.  Repeat until we find no match for the
                    // input immediately after what was consumed by the last EmitCopy call.
                    //
                    // If we exit this loop normally then we need to call EmitLiteral next,
                    // though we don't yet know how big the literal will be.  We handle that
                    // by proceeding to the next iteration of the main loop.  We also can exit
                    // this loop via goto if we get close to exhausting the input.
                    uint candidateBytes;
                    int insertTail;

                    do
                    {
                        // We have a 4-byte match at ip, and no need to emit any
                        // "literal bytes" prior to ip.
                        var baseIndex = inputIndex;
                        var matched = 4 + _findMatchLength(input, candidate + 4, inputIndex + 4, inputEnd);
                        inputIndex += matched;
                        var offset = baseIndex - candidate;
                        //DCHECK_EQ(0, memcmp(baseIndex, candidate, matched));
                        outputIndex = EmitCopy(output, outputIndex, offset, matched);
                        // We could immediately start working at ip now, but to improve
                        // compression we first update table[Hash(ip - 1, ...)].
                        insertTail = inputIndex - 1;
                        nextEmitIndex = inputIndex;
                        if (inputIndex >= ipLimit)
                        {
                            goto emit_remainder;
                        }
                        var prevHash = Hash(Utilities.GetFourBytes(input, insertTail), shift);
                        hashTable[prevHash] = (short) (inputIndex - baseInputIndex - 1);
                        var curHash = Hash(Utilities.GetFourBytes(input, insertTail + 1), shift);
                        candidate = baseInputIndex + hashTable[curHash];
                        candidateBytes = Utilities.GetFourBytes(input, candidate);
                        hashTable[curHash] = (short) (inputIndex - baseInputIndex);
                    } while (Utilities.GetFourBytes(input, insertTail + 1) == candidateBytes);

                    nextHash = Hash(Utilities.GetFourBytes(input, insertTail + 2), shift);
                    ++inputIndex;
                }
            }

            emit_remainder:
            // Emit the remaining bytes as a literal
            if (nextEmitIndex < inputEnd)
            {
                outputIndex = EmitLiteral(output, outputIndex, input, nextEmitIndex, inputEnd - nextEmitIndex, false);
            }

            return outputIndex;
        }

        private static int EmitCopyLessThan64(byte[] output, int outputIndex, int offset, int length)
        {
            Debug.Assert(offset >= 0);
            Debug.Assert(length <= 64);
            Debug.Assert(length >= 4);
            Debug.Assert(offset < 65536);

            if (length < 12 && offset < 2048)
            {
                var lenMinus4 = length - 4;
                Debug.Assert(lenMinus4 < 8); // Must fit in 3 bits
                output[outputIndex++] = (byte) (Snappy.Copy1ByteOffset | (lenMinus4 << 2) | ((offset >> 8) << 5));
                output[outputIndex++] = (byte) offset;
            }
            else
            {
                output[outputIndex++] = (byte) (Snappy.Copy2ByteOffset | ((length - 1) << 2));
                output[outputIndex++] = (byte) (offset);
                output[outputIndex++] = (byte) (offset >> 8);
            }
            return outputIndex;
        }

        private static int EmitCopy(byte[] compressed, int compressedIndex, int offset, int length)
        {
            // Emit 64 byte copies but make sure to keep at least four bytes reserved
            while (length >= 68)
            {
                compressedIndex = EmitCopyLessThan64(compressed, compressedIndex, offset, 64);
                length -= 64;
            }

            // Emit an extra 60 byte copy if have too much data to fit in one copy
            if (length > 64)
            {
                compressedIndex = EmitCopyLessThan64(compressed, compressedIndex, offset, 60);
                length -= 60;
            }

            // Emit remainder
            compressedIndex = EmitCopyLessThan64(compressed, compressedIndex, offset, length);
            return compressedIndex;
        }

        // Return the largest n such that
        //
        //   s1[s1Index,n-1] == s1[s2Index,n-1]
        //   and n <= (s2Limit - s2Index).
        //
        // Does not read s2Limit or beyond.
        // Does not read *(s1 + (s2_limit - s2)) or beyond.
        // Requires that s2Limit >= s2.
        private static int FindMatchLengthBasic(byte[] s1, int s1Index, int s2Index, int s2Limit)
        {
            Debug.Assert(s2Limit >= s2Index);
            var matched = 0;
            while (s2Index + matched < s2Limit && s1[s1Index + matched] == s1[s2Index + matched])
            {
                ++matched;
            }
            return matched;
        }

        // 32-bit optimized version of above
        private static int FindMatchLength32(byte[] s1, int s1Index, int s2Index, int s2Limit)
        {
            Debug.Assert(s2Limit >= s2Index);

            var matched = 0;
            while (s2Index <= s2Limit - 4)
            {
                var a = Utilities.GetFourBytes(s1, s2Index);
                var b = Utilities.GetFourBytes(s1, s1Index + matched);

                if (a == b)
                {
                    s2Index += 4;
                    matched += 4;
                }
                else
                {
                    var c = a ^ b;
                    var matchingBits = (int) Utilities.NumberOfTrailingZeros(c);
                    matched += matchingBits >> 3;
                    return matched;
                }
            }
            while (s2Index < s2Limit)
            {
                if (s1[s1Index + matched] == s1[s2Index])
                {
                    ++s2Index;
                    ++matched;
                }
                else
                {
                    return matched;
                }
            }
            return matched;
        }


        // 64-bit optimized version of above
        private static int FindMatchLength64(byte[] s1, int s1Index, int s2Index, int s2Limit)
        {
            Debug.Assert(s2Limit >= s2Index);

            var matched = 0;
            while (s2Index <= s2Limit - 8)
            {
                var a = Utilities.GetEightBytes(s1, s2Index);
                var b = Utilities.GetEightBytes(s1, s1Index + matched);

                if (a == b)
                {
                    s2Index += 8;
                    matched += 8;
                }
                else
                {
                    var c = a ^ b;
                    // first get low order 32 bits, if all 0 then get high order as well.
                    var matchingBits = (int) Utilities.NumberOfTrailingZeros(c);
                    matched += matchingBits >> 3;
                    return matched;
                }
            }
            while (s2Index < s2Limit)
            {
                if (s1[s1Index + matched] == s1[s2Index])
                {
                    ++s2Index;
                    ++matched;
                }
                else
                {
                    return matched;
                }
            }
            return matched;
        }


		public int EmitLiteral(byte[] output, int outputIndex, byte[] literal, int literalIndex, int length,
            bool allowFastPath)
        {
            var n = length - 1;
            outputIndex = EmitLiteralTag(output, outputIndex, n);
            if (allowFastPath && length <= 16)
            {
                Utilities.UnalignedCopy64(literal, literalIndex, output, outputIndex);
                Utilities.UnalignedCopy64(literal, literalIndex + 8, output, outputIndex + 8);
                return outputIndex + length;
            }
            Buffer.BlockCopy(literal, literalIndex, output, outputIndex, length);
            return outputIndex + length;
        }

        public int EmitLiteralTag(byte[] output, int outputIndex, int size)
        {
            if (size < 60)
            {
                output[outputIndex++] = (byte) (Snappy.Literal | (size << 2));
            }
            else
            {
                var baseIndex = outputIndex;
                outputIndex++;
                // TODO: Java version is 'unrolled' here, C++ isn't. Should look into it?
                var count = 0;
                while (size > 0)
                {
                    output[outputIndex++] = (byte) (size & 0xff);
                    size >>= 8;
                    count++;
                }
                Debug.Assert(count >= 1);
                Debug.Assert(count <= 4);
                output[baseIndex] = (byte) (Snappy.Literal | ((59 + count) << 2));
            }
            return outputIndex;
        }

        internal int GetHashTableSize(int inputSize)
        {
            // Use smaller hash table when input.size() is smaller, since we
            // fill the table, incurring O(hash table size) overhead for
            // compression, and if the input is short, we won't need that
            // many hash table entries anyway.
            Debug.Assert(MaxHashTableSize >= 256);

            var hashTableSize = 256;
            // TODO: again, java version unrolled, but this time with note that it isn't faster
            while (hashTableSize < MaxHashTableSize && hashTableSize < inputSize)
            {
                hashTableSize <<= 1;
            }
            Debug.Assert(0 == (hashTableSize & (hashTableSize - 1)), "hash must be power of two");
            Debug.Assert(hashTableSize <= MaxHashTableSize, "hash table too large");
            return hashTableSize;
        }

        private static uint Hash(uint bytes, int shift)
        {
            const int kMul = 0x1e35a7bd;
            return (bytes * kMul) >> shift;
        }

		public int WriteUncomressedLength(byte[] compressed, int compressedOffset, int uncompressedLength)
        {
            const int bitMask = 0x80;
            if (uncompressedLength < 0)
                throw new ArgumentException("uncompressedLength");

            // A little-endian varint. 
            // From doc:
            // Varints consist of a series of bytes, where the lower 7 bits are data and the upper bit is set iff there are more bytes to read.
            // In other words, an uncompressed length of 64 would be stored as 0x40, and an uncompressed length of 2097150 (0x1FFFFE) would
            // be stored as 0xFE 0XFF 0X7F
            while (uncompressedLength >= bitMask)
            {
                compressed[compressedOffset++] = (byte) (uncompressedLength | bitMask);
                uncompressedLength = uncompressedLength >> 7;
            }
            compressed[compressedOffset++] = (byte) uncompressedLength;

            return compressedOffset;
        }

        internal short[] GetHashTable(int inputLength)
        {
            // Use smaller hash table when input.size() is smaller, since we
            // fill the table, incurring O(hash table size) overhead for
            // compression, and if the input is short, we won't need that
            // many hash table entries anyway.
            Debug.Assert(MaxHashTableSize > 256);
            var tableSize = 256;
            while (tableSize < MaxHashTableSize && tableSize < inputLength)
            {
                tableSize <<= 1;
            }
            Debug.Assert((tableSize & (tableSize - 1)) == 0, "Table size not power of 2.");
            Debug.Assert(tableSize <= MaxHashTableSize, "Table size too large.");
            // TODO: C++/Java versions do this with a reusable buffer for efficiency. Probably also useful here. All that not allocating in a tight loop and all
            return new short[tableSize];
        }
    }
}