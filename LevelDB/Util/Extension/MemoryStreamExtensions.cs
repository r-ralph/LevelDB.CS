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

namespace LevelDB.Util.Extension
{
    public static class MemoryStreamExtensions
    {
        public static void Clear(this MemoryStream stream)
        {
			stream.Position = 0;
        }

        public static long Remaining(this MemoryStream stream)
        {
            return stream.Length - stream.Position;
        }

        public static void Put(this MemoryStream stream, byte[] buffer, int offset, int count)
        {
            stream.Write(buffer, offset, count);
        }

        public static MemoryStream Duplicate(this MemoryStream ms)
        {
            var pos = ms.Position;
            var ms2 = new MemoryStream();
            ms.CopyTo(ms2);
            ms.Position = pos;
            ms2.Position = pos;
            return ms2;
        }
    }
}