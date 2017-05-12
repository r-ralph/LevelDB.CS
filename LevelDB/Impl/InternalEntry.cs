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

using System.Text;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class InternalEntry : Entry<InternalKey, Slice>
    {
        public override InternalKey Key { get; }
        public override Slice Value { get; }

        public InternalEntry(InternalKey key, Slice value)
        {
            Preconditions.CheckNotNull(key, $"{nameof(key)} is null");
            Preconditions.CheckNotNull(value, $"{nameof(value)} is null");
            Key = key;
            Value = value;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            if (!(o is InternalEntry))
            {
                return false;
            }
            var entry = (InternalEntry) o;
            return Key.Equals(entry.Key) && Value.Equals(entry.Value);
        }

        public override int GetHashCode()
        {
            var result = Key.GetHashCode();
            result = 31 * result + Value.GetHashCode();
            return result;
        }

        public override string ToString()
        {
            return $"BlockEntry(key={Key}, value={Value.ToString(Encoding.UTF8)}))";
        }
    }
}