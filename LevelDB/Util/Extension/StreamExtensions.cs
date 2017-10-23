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
using System.IO.MemoryMappedFiles;

namespace LevelDB.Util.Extension
{
    public static class StreamExtensions
    {
        public static void Clear(this Stream stream)
        {
            stream.Position = 0;
        }

        public static MemoryStream Duplicate(this Stream ms)
        {
            var pos = ms.Position;
            var ms2 = new MemoryStream();
            ms.Position = 0;
            ms.CopyTo(ms2);
            ms.Position = pos;
            ms2.Position = pos;
            ms2.SetLength(ms.Length);
            return ms2;
        }

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