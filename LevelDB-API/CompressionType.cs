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

namespace LevelDB
{
    public class CompressionType
    {
        public static readonly CompressionType None = new CompressionType(0x00);
        public static readonly CompressionType Snappy = new CompressionType(0x01);
        public static readonly CompressionType Zlib = new CompressionType(0x02);

        public static IEnumerable<CompressionType> Values
        {
            get
            {
                yield return None;
                yield return Snappy;
                yield return Zlib;
            }
        }

        public int PersistentId { get; }

        public static CompressionType GetCompressionTypeByPersistentId(int persistentId)
        {
            foreach (var compressionType in Values)
            {
                if (compressionType.PersistentId == persistentId)
                {
                    return compressionType;
                }
            }
            throw new ArgumentException("Unknown persistent id :" + persistentId);
        }

        public CompressionType(int persistentId)
        {
            PersistentId = persistentId;
        }
    }
}