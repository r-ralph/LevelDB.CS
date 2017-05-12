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

namespace LevelDB.Table
{
    /// <summary>
    /// Binary Structure
    /// <table summary="record format">
    /// <tbody>
    /// <thead>
    /// <tr>
    /// <th>name</th>
    /// <th>offset</th>
    /// <th>length</th>
    /// <th>description</th>
    /// </tr>
    /// </thead>
    /// <p/>
    /// <tr>
    /// <td>shared key length</td>
    /// <td>0</td>
    /// <td>vary</td>
    /// <td>variable length encoded int: size of shared key prefix with the key from the previous entry</td>
    /// </tr>
    /// <tr>
    /// <td>non-shared key length</td>
    /// <td>vary</td>
    /// <td>vary</td>
    /// <td>variable length encoded int: size of non-shared key suffix in this entry</td>
    /// </tr>
    /// <tr>
    /// <td>value length</td>
    /// <td>vary</td>
    /// <td>vary</td>
    /// <td>variable length encoded int: size of value in this entry</td>
    /// </tr>
    /// <tr>
    /// <td>non-shared key</td>
    /// <td>vary</td>
    /// <td>non-shared key length</td>
    /// <td>non-shared key data</td>
    /// </tr>
    /// <tr>
    /// <td>value</td>
    /// <td>vary</td>
    /// <td>value length</td>
    /// <td>value data</td>
    /// </tr>
    /// </tbody>
    /// </table>
    /// </summary>
    public class BlockEntry : Entry<Slice, Slice>
    {
        public override Slice Key { get; }
        public override Slice Value { get; }

        public BlockEntry(Slice key, Slice value)
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
            if (!(o is BlockEntry))
            {
                return false;
            }
            var entry = (BlockEntry) o;
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
            return $"BlockEntry(key={Key.ToString(Encoding.UTF8)}, value={Value.ToString(Encoding.UTF8)}))";
        }
    }
}