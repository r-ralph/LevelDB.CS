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

using LevelDB.Table;

namespace LevelDB.Util
{
    public sealed class TableIterator : AbstractSeekingIterator<Slice, Slice>
    {
        private readonly Table.Table _table;
        private readonly BlockIterator _blockIterator;
        private BlockIterator _current;

        public TableIterator(Table.Table table, BlockIterator blockIterator)
        {
            _table = table;
            _blockIterator = blockIterator;
            _current = null;
        }

        protected override void SeekToFirstInternal()
        {
            // reset index to before first and clear the data iterator
            _blockIterator.SeekToFirst();
            _current = null;
        }

        protected override void SeekInternal(Slice targetKey)
        {
            // seek the index to the block containing the key
            _blockIterator.Seek(targetKey);

            // if indexIterator does not have a next, it mean the key does not exist in this iterator
            if (_blockIterator.HasNext())
            {
                // seek the current iterator to the key
                _current = GetNextBlock();
                _current.Seek(targetKey);
            }
            else
            {
                _current = null;
            }
        }

        protected override Entry<Slice, Slice> GetNextElement()
        {
            // note: it must be here & not where 'current' is assigned,
            // because otherwise we'll have called inputs.next() before throwing
            // the first NPE, and the next time around we'll call inputs.next()
            // again, incorrectly moving beyond the error.
            var currentHasNext = false;
            while (true)
            {
                if (_current != null)
                {
                    currentHasNext = _current.HasNext();
                }
                if (!currentHasNext)
                {
                    if (_blockIterator.HasNext())
                    {
                        _current = GetNextBlock();
                    }
                    else
                    {
                        break;
                    }
                }
                else
                {
                    break;
                }
            }
            if (currentHasNext)
            {
                return _current.Next();
            }
            // set current to empty iterator to avoid extra calls to user iterators
            _current = null;
            return null;
        }

        private BlockIterator GetNextBlock()
        {
            var blockHandle = _blockIterator.Next().Value;
            var dataBlock = _table.OpenBlock(blockHandle);
            return dataBlock.GetBlockIterator();
        }

        public override string ToString()
        {
            return $"ConcatenatingIterator(blockIterator={_blockIterator}, current={_current})";
        }
    }
}