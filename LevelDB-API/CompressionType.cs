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

        private static readonly IDictionary<int, CompressionType> Mapping = new Dictionary<int, CompressionType>();

        static CompressionType()
        {
            Register(None);
            Register(Snappy);
        }

        public static void Register(CompressionType compressionType)
        {
            if (Mapping.ContainsKey(compressionType.PersistentId))
            {
                throw new InvalidOperationException(
                    $"Trying to register same CompressionType(id={compressionType.PersistentId})");
            }
            Mapping.Add(compressionType.PersistentId, compressionType);
        }

        public byte PersistentId { get; }

        public static CompressionType GetCompressionTypeByPersistentId(int persistentId)
        {
            foreach (var entry in Mapping)
            {
                if (entry.Key == persistentId)
                {
                    return entry.Value;
                }
            }
            throw new ArgumentException("Unknown persistent id :" + persistentId);
        }

        public CompressionType(byte persistentId)
        {
            PersistentId = persistentId;
        }
    }
}