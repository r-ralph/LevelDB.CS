﻿using System;
using System.Diagnostics;

namespace Snappy.Sharp
{
	public class Utilities
    {
        public static unsafe int NativeIntPtrSize()
        {
            return sizeof(IntPtr);
        }

        /// <summary>
        /// Copies 64 bits (8 bytes) from source array starting at sourceIndex into dest array starting at destIndex.
        /// </summary>
        /// <param name="source">The source array.</param>
        /// <param name="sourceIndex">Index to start copying.</param>
        /// <param name="dest">The destination array.</param>
        /// <param name="destIndex">Index to start writing.</param>
        /// <remarks>The name comes from the original Snappy C++ source. I don't think there is a good way to look at
        /// things in an aligned manner in the .NET Framework.</remarks>
        //[SecuritySafeCritical]
        public static unsafe void UnalignedCopy64(byte[] source, int sourceIndex, byte[] dest, int destIndex)
        {
            Debug.Assert(sourceIndex > -1);
            Debug.Assert(destIndex > -1);
            Debug.Assert(sourceIndex + 7 < source.Length);
            Debug.Assert(destIndex + 7 < dest.Length);

            fixed (byte* src = &source[sourceIndex], dst = &dest[destIndex])
            {
                *((long*) dst) = *((long*) src);
            }
        }

        ///<summary>>
        /// Reads 4 bytes from memory into a uint. Does not take host enianness into account.
        /// </summary>
        //[SecuritySafeCritical]
        public static unsafe uint GetFourBytes(byte[] source, int index)
        {
            Debug.Assert(index > -1);
            Debug.Assert(index + 3 < source.Length);

            fixed (byte* src = &source[index])
            {
                return *((uint*) src);
            }
        }

        ///<summary>>
        /// Reads 8 bytes from memory into a uint. Does not take host enianness into account.
        /// </summary>
        //[SecuritySafeCritical]
        public static unsafe ulong GetEightBytes(byte[] source, int index)
        {
            Debug.Assert(index > -1);
            Debug.Assert(index + 7 < source.Length);

            fixed (byte* src = &source[index])
            {
                return *((ulong*) src);
            }
        }

        // Function from http://aggregate.org/MAGIC/
        public static uint Log2Floor(uint x)
        {
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            // here Log2Floor(0) = 0
            return NumberOfOnes(x >> 1);
        }

        // Function from http://aggregate.org/MAGIC/
        public static uint NumberOfLeadingZeros(uint x)
        {
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            return sizeof(int) * 8 - NumberOfOnes(x);
        }

        // Function from http://aggregate.org/MAGIC/
        public static uint NumberOfTrailingZeros(uint x)
        {
            return NumberOfOnes((uint) ((x & -x) - 1));
        }

        // Function from http://aggregate.org/MAGIC/
        public static uint NumberOfOnes(uint x)
        {
            x -= (x >> 1) & 0x55555555;
            x = ((x >> 2) & 0x33333333) + (x & 0x33333333);
            x = ((x >> 4) + x) & 0x0f0f0f0f;
            x += x >> 8;
            x += x >> 16;
            return x & 0x0000003f;
        }

        public static uint NumberOfTrailingZeros(ulong c)
        {
            var matchingBits = NumberOfTrailingZeros((uint) c);
            if (matchingBits == 32)
            {
                matchingBits += NumberOfTrailingZeros((uint) (c >> 32));
            }
            return matchingBits;
        }
    }
}