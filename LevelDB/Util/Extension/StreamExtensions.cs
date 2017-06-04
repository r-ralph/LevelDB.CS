using System;
using System.IO;

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
    }
}