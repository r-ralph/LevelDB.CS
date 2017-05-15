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
using Ionic.Zlib;
using LevelDB.Util;
using LevelDB.Util.Extension;

namespace LevelDB
{
    public static class LevelDBZlib
    {
        public static readonly CompressionType Zlib = new CompressionType(0x02);

        public static void Init()
        {
            Compressions.Register(Zlib,
                (source, sourceOffset, sourceLength, dest) =>
                {
                    var compressed = ZlibStream.CompressBuffer(source.SubArray(sourceOffset, sourceLength));
                    Array.Copy(compressed, 0, dest, 0, compressed.Length);
                    return compressed.Length;
                },
                (source, dest) =>
                {
                    var decompressed = ZlibStream.UncompressBuffer(source.ToArray());
                    dest.Write(decompressed, 0, decompressed.Length);
                    dest.Position = 0;
                    dest.SetLength(decompressed.Length);
                });
        }
    }
}