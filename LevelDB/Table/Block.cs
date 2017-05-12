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

using System.Collections;
using System.Collections.Generic;
using LevelDB.Guava;
using LevelDB.Impl;
using LevelDB.Util;

namespace LevelDB.Table
{
    ///
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
    /// <td>entries</td>
    /// <td>4</td>
    /// <td>vary</td>
    /// <td>Entries in order by key</td>
    /// </tr>
    /// <tr>
    /// <td>restart index</td>
    /// <td>vary</td>
    /// <td>4 * restart count</td>
    /// <td>Index of prefix compression restarts</td>
    /// </tr>
    /// <tr>
    /// <td>restart count</td>
    /// <td>0</td>
    /// <td>4</td>
    /// <td>Number of prefix compression restarts (used as index into entries)</td>
    /// </tr>
    /// </tbody>
    /// </table>
    /// </summary>
    ///
    public class Block : ISeekingIterable<Slice, Slice>
    {
        private readonly Slice _block;
        private readonly IComparer<Slice> _comparer;

        private readonly Slice _data;
        private readonly Slice _restartPositions;


        public Block(Slice block, IComparer<Slice> comparer)
        {
            Preconditions.CheckNotNull(block, "block is null");
            Preconditions.CheckArgument(block.Length >= SizeOf.Int, "Block is corrupt: size must be at least %s block",
                SizeOf.Int);
            Preconditions.CheckNotNull(comparer, "comparator is null");

            block = block.Sliced();
            _block = block;
            _comparer = comparer;

            // Keys are prefix compressed.  Every once in a while the prefix compression is restarted and the full key is written.
            // These "restart" locations are written at the end of the file, so you can seek to key without having to read the
            // entire file sequentially.

            // key restart count is the last int of the block
            var restartCount = block.GetInt(block.Length - SizeOf.Int);

            if (restartCount > 0)
            {
                // restarts are written at the end of the block
                var restartOffset = block.Length - (1 + restartCount) * SizeOf.Int;
                Preconditions.CheckArgument(restartOffset < block.Length - SizeOf.Int,
                    "Block is corrupt: restart offset count is greater than block size");
                _restartPositions = block.Sliced(restartOffset, restartCount * SizeOf.Int);

                // data starts at 0 and extends to the restart index
                _data = block.Sliced(0, restartOffset);
            }
            else
            {
                _data = Slices.EmptySlice;
                _restartPositions = Slices.EmptySlice;
            }
        }

        public long Size => _block.Length;


        public BlockIterator GetBlockIterator()
        {
            return new BlockIterator(_data, _restartPositions, _comparer);
        }

        public IEnumerator<Entry<Slice, Slice>> GetEnumerator()
        {
            return GetBlockIterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}