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
using System.Collections;
using System.Collections.Generic;
using LevelDB.Guava;
using LevelDB.Impl;
using LevelDB.Util;

namespace LevelDB.Table
{
    public class BlockIterator : ISeekingIterator<Slice, Slice>
    {
        private readonly SliceInput _data;
        private readonly Slice _restartPositions;
        private readonly int _restartCount;
        private readonly IComparer<Slice> _comparer;
        private BlockEntry _currentEntry;
        private BlockEntry _nextEntry;

        public Entry<Slice, Slice> Current => _currentEntry;
        object IEnumerator.Current => Current;

        public BlockIterator(Slice data, Slice restartPositions, IComparer<Slice> comparer)
        {
            Preconditions.CheckNotNull(data, "data is null");
            Preconditions.CheckNotNull(restartPositions, "restartPositions is null");
            Preconditions.CheckArgument(restartPositions.Length % SizeOf.Int == 0,
                "restartPositions.readableBytes() must be a multiple of %s", SizeOf.Int);
            Preconditions.CheckNotNull(comparer, "comparer is null");
            _data = data.Input();
            _restartPositions = restartPositions.Sliced();
            _restartCount = _restartPositions.Length / SizeOf.Int;
            _comparer = comparer;
            SeekToFirst();
        }

        public bool HasNext()
        {
            return _nextEntry != null;
        }

        public Entry<Slice, Slice> Peek()
        {
            if (!HasNext())
            {
                throw new InvalidOperationException("no element");
            }
            return _nextEntry;
        }

        public Entry<Slice, Slice> Next()
        {
            if (!HasNext())
            {
                throw new InvalidOperationException("no element");
            }
            var entry = _nextEntry;
            _currentEntry = _nextEntry;

            _nextEntry = !_data.CanRead ? null : ReadEntry(_data, _nextEntry);

            return entry;
        }

        /// <summary>
        /// Repositions the iterator so the beginning of this block.
        /// </summary>
        public void SeekToFirst()
        {
            if (_restartCount > 0)
            {
                SeekToRestartPosition(0);
            }
        }

        /// <summary>
        /// Repositions the iterator so the key of the next BlockElement returned greater than or equal to the specified targetKey.
        /// </summary>
        /// <param name="targetKey">The target's key</param>
        public void Seek(Slice targetKey)
        {
            if (_restartCount == 0)
            {
                return;
            }

            var left = 0;
            var right = _restartCount - 1;

            // binary search restart positions to find the restart position immediately before the targetKey
            while (left < right)
            {
                var mid = (left + right + 1) / 2;

                SeekToRestartPosition(mid);

                if (_comparer.Compare(_nextEntry.Key, targetKey) < 0)
                {
                    // key at mid is smaller than targetKey.  Therefore all restart
                    // blocks before mid are uninteresting.
                    left = mid;
                }
                else
                {
                    // key at mid is greater than or equal to targetKey.  Therefore
                    // all restart blocks at or after mid are uninteresting.
                    right = mid - 1;
                }
            }

            // linear search (within restart block) for first key greater than or equal to targetKey
            for (SeekToRestartPosition(left); _nextEntry != null; Next())
            {
                if (_comparer.Compare(Peek().Key, targetKey) >= 0)
                {
                    break;
                }
            }
        }

        /// <summary>
        /// Seeks to and reads the entry at the specified restart position.
        /// After this method, nextEntry will contain the next entry to return, and the previousEntry will be null.
        /// </summary>
        /// <param name="restartPosition"></param>
        private void SeekToRestartPosition(int restartPosition)
        {
            Preconditions.CheckPositionIndex(restartPosition, _restartCount, nameof(restartPosition));
            // seek data readIndex to the beginning of the restart block
            var offset = _restartPositions.GetInt(restartPosition * SizeOf.Int);
            _data.Position = offset;
            // clear the entries to assure key is not prefixed
            _nextEntry = null;
            // read the entry
            _nextEntry = ReadEntry(_data, null);
        }

        /// <summary>
        /// Reads the entry at the current data readIndex.
        /// After this method, data readIndex is positioned at the beginning of the next entry
        /// or at the end of data if there was not a next entry.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="previousEntry"></param>
        /// <returns></returns>
        private static BlockEntry ReadEntry(SliceInput data, BlockEntry previousEntry)
        {
            Preconditions.CheckNotNull(data, "data is null");

            // read entry header
            var sharedKeyLength = (int) VariableLengthQuantity.ReadVariableLengthInt(data);
            var nonSharedKeyLength = (int) VariableLengthQuantity.ReadVariableLengthInt(data);
            var valueLength = (int) VariableLengthQuantity.ReadVariableLengthInt(data);

            // read key
            var key = Slices.Allocate(sharedKeyLength + nonSharedKeyLength);
            var sliceOutput = key.Output();
            if (sharedKeyLength > 0)
            {
                Preconditions.CheckState(previousEntry != null,
                    "Entry has a shared key but no previous entry was provided");
                // ReSharper disable once PossibleNullReferenceException
                sliceOutput.WriteBytes(previousEntry.Key, 0, sharedKeyLength);
            }
            sliceOutput.WriteBytes(data, nonSharedKeyLength);

            // read value
            var value = data.ReadSlice(valueLength);

            return new BlockEntry(key, value);
        }

        public void Reset()
        {
            SeekToFirst();
        }

        #region UnSupported methods

        public bool MoveNext()
        {
            throw new NotSupportedException();
        }

        public Entry<Slice, Slice> Remove()
        {
            throw new NotSupportedException();
        }

        public void Dispose()
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}