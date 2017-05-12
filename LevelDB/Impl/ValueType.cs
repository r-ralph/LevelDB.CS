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

namespace LevelDB.Impl
{
    public sealed class ValueType
    {
        public static readonly ValueType Deletion = new ValueType(0x00);
        public static readonly ValueType Value = new ValueType(0x01);

        public static ValueType GetValueTypeByPersistentId(int persistentId)
        {
            switch (persistentId)
            {
                case 0:
                    return Deletion;
                case 1:
                    return Value;
                default:
                    throw new ArgumentException($"Unknown persistentId :{persistentId}");
            }
        }

        public int PersistentId { get; }

        private ValueType(int persistentId)
        {
            PersistentId = persistentId;
        }
    }
}