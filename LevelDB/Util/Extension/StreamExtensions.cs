using System;
using System.IO;
using System.IO.MemoryMappedFiles;

namespace LevelDB.Util.Extension
{
    public static class StreamExtensions
    {
        public static void Fill(this Stream stream, byte value, int count)
        {
            var buffer = new byte[64];
            for (var i = 0; i < buffer.Length; i++)
            {
                buffer[i] = value;
            }
            while (count > buffer.Length)
            {
                stream.Write(buffer, 0, buffer.Length);
                count -= buffer.Length;
            }
            stream.Write(buffer, 0, count);
        }

        public static bool IsEof(this Stream stream)
        {
            return stream.Position == stream.Length;
        }

        public static long Remaining(this Stream stream)
        {
            return stream.Length - stream.Position;
        }

        public static long Remaining(this MemoryMappedViewAccessor stream)
        {
            return stream.Capacity - stream.PointerOffset;
        }

        public static void Put(this Stream stream, byte[] buffer, int offset, int count)
        {
            stream.Write(buffer, offset, count);
        }

        public static void Put(this MemoryMappedViewAccessor stream, byte[] buffer, int offset, int count)
        {
            stream.WriteArray(stream.PointerOffset, buffer, offset, count);
        }
    }
}