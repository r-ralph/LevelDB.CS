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
using System.Collections.Generic;
using System.IO;
using LevelDB.Guava;
using Snappy.Sharp;

namespace LevelDB.Util
{
    public static class Compressions
    {
        private static readonly Dictionary<int, Func<byte[], int, int, byte[], long>> CompressionMethods =
            new Dictionary<int, Func<byte[], int, int, byte[], long>>();

        private static readonly Dictionary<int, Action<MemoryStream, MemoryStream>> DecompressionMethods =
            new Dictionary<int, Action<MemoryStream, MemoryStream>>();

        static Compressions()
        {
            // None
            CompressionMethods.Add(CompressionType.None.PersistentId, (source, sourceOffset, sourceLength, dest) =>
            {
                Array.Copy(source, sourceOffset, dest, 0, sourceLength);
                return source.Length;
            });
            DecompressionMethods.Add(CompressionType.None.PersistentId, (source, dest) =>
            {
                source.CopyTo(dest);
                dest.Position = 0;
                dest.SetLength(source.Length);
            });

            //Snappy
            CompressionMethods.Add(CompressionType.Snappy.PersistentId, (source, sourceOffset, sourceLength, dest) =>
                new SnappyCompressor().Compress(source, sourceOffset, sourceLength, dest, 0));
            DecompressionMethods.Add(CompressionType.Snappy.PersistentId, (source, dest) =>
            {
                var pos = source.Position;
                var size = (int)(source.Length - pos);
                var arr = new byte[size];
                source.Read(arr, 0, size);
                var decompressed = new SnappyDecompressor().Decompress(arr, 0, size);
                dest.Write(decompressed, 0, decompressed.Length);
                dest.Position = 0;
                dest.SetLength(decompressed.Length);
            });
        }

        public static void Register(CompressionType type, Func<byte[], int, int, byte[], long> compressFunc,
            Action<MemoryStream, MemoryStream> decompressFunc)
        {
            Preconditions.CheckState(!CompressionMethods.ContainsKey(type.PersistentId),
                $"Trying to register same CompressionType(id={type.PersistentId})");
            CompressionType.Register(type);
            CompressionMethods[type.PersistentId] = compressFunc;
            DecompressionMethods[type.PersistentId] = decompressFunc;
        }

        public static long Compress(CompressionType type, Slice source, Slice dest)
        {
            return Compress(type, source.GetRawArray(), source.GetRawOffset(), source.Length, dest.GetRawArray());
        }

        public static long Compress(CompressionType type, byte[] source, int sourceOffset, int sourceLength,
            byte[] dest)
        {
            if (CompressionMethods.ContainsKey(type.PersistentId))
            {
                return CompressionMethods[type.PersistentId].Invoke(source, sourceOffset, sourceLength, dest);
            }
            throw new InvalidOperationException($"Unknown CompressionType: {type.PersistentId}");
        }

        public static void Decompress(CompressionType type, MemoryStream source, MemoryStream dest)
        {
            if (DecompressionMethods.ContainsKey(type.PersistentId))
            {
                DecompressionMethods[type.PersistentId].Invoke(source, dest);
            }
            else
            {
                throw new InvalidOperationException($"Unknown CompressionType: {type.PersistentId}");
            }
        }
    }
}