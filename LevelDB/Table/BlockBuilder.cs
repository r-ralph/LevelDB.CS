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
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Table
{
    public class BlockBuilder
    {
        private readonly int _blockRestartInterval;
        private readonly IntVector _restartPositions;
        private readonly IComparer<Slice> _comparator;

        private int _entryCount;
        private int _restartBlockEntryCount;

        private bool _finished;
        private readonly DynamicSliceOutput _block;
        private Slice _lastKey;

        public BlockBuilder(int estimatedSize, int blockRestartInterval, IComparer<Slice> comparator)
        {
            Preconditions.CheckArgument(estimatedSize >= 0, $"{nameof(estimatedSize)} is negative");
            Preconditions.CheckArgument(blockRestartInterval >= 0, $"{nameof(blockRestartInterval)} is negative");
            Preconditions.CheckNotNull(comparator, $"{nameof(comparator)} is null");

            _block = new DynamicSliceOutput(estimatedSize);
            _blockRestartInterval = blockRestartInterval;
            _comparator = comparator;

            _restartPositions = new IntVector(32);
            _restartPositions.Add(0); // first restart point must be 0
        }

        public void Reset()
        {
            _block.Reset();
            _entryCount = 0;
            _restartPositions.Clear();
            _restartPositions.Add(0); // first restart point must be 0
            _restartBlockEntryCount = 0;
            _lastKey = null;
            _finished = false;
        }

        public int GetEntryCount()
        {
            return _entryCount;
        }

        public bool IsEmpty()
        {
            return _entryCount == 0;
        }

        public int CurrentSizeEstimate()
        {
            // no need to estimate if closed
            if (_finished)
            {
                return _block.Size();
            }

            // no records is just a single int
            if (_block.Size() == 0)
            {
                return SizeOf.Int;
            }

            return _block.Size() + // raw data buffer
                   _restartPositions.Size * SizeOf.Int + // restart positions
                   SizeOf.Int; // restart position size
        }

        public void Add(BlockEntry blockEntry)
        {
            Preconditions.CheckNotNull(blockEntry, $"{nameof(blockEntry)} is null");
            Add(blockEntry.Key, blockEntry.Value);
        }

        public void Add(Slice key, Slice value)
        {
            Preconditions.CheckNotNull(key, $"{nameof(key)} is null");
            Preconditions.CheckNotNull(value, $"{nameof(value)} is null");
            Preconditions.CheckState(!_finished, $"{nameof(_block)} is finished");
            Preconditions.CheckPositionIndex(_restartBlockEntryCount, _blockRestartInterval);

            Preconditions.CheckArgument(_lastKey == null || _comparator.Compare(key, _lastKey) > 0,
                "key must be greater than last key");

            var sharedKeyBytes = 0;
            if (_restartBlockEntryCount < _blockRestartInterval)
            {
                sharedKeyBytes = CalculateSharedBytes(key, _lastKey);
            }
            else
            {
                // restart prefix compression
                _restartPositions.Add(_block.Size());
                _restartBlockEntryCount = 0;
            }

            var nonSharedKeyBytes = key.Length - sharedKeyBytes;

            // write "<shared><non_shared><value_size>"
            VariableLengthQuantity.WriteVariableLengthInt((uint) sharedKeyBytes, _block);
            VariableLengthQuantity.WriteVariableLengthInt((uint) nonSharedKeyBytes, _block);
            VariableLengthQuantity.WriteVariableLengthInt((uint) value.Length, _block);

            // write non-shared key bytes
            _block.WriteBytes(key, sharedKeyBytes, nonSharedKeyBytes);

            // write value bytes
            _block.WriteBytes(value, 0, value.Length);

            // update last key
            _lastKey = key;

            // update state
            _entryCount++;
            _restartBlockEntryCount++;
        }

        public static int CalculateSharedBytes(Slice leftKey, Slice rightKey)
        {
            var sharedKeyBytes = 0;

            if (leftKey == null || rightKey == null) return sharedKeyBytes;
            var minSharedKeyBytes = Math.Min(leftKey.Length, rightKey.Length);
            while (sharedKeyBytes < minSharedKeyBytes &&
                   leftKey.GetByte(sharedKeyBytes) == rightKey.GetByte(sharedKeyBytes))
            {
                sharedKeyBytes++;
            }
            return sharedKeyBytes;
        }

        public Slice Finish()
        {
            if (_finished) return _block.Sliced();
            _finished = true;

            if (_entryCount > 0)
            {
                _restartPositions.Write(_block);
                _block.WriteInt(_restartPositions.Size);
            }
            else
            {
                _block.WriteInt(0);
            }
            return _block.Sliced();
        }
    }
}