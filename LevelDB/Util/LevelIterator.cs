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

using System.Collections.Generic;
using LevelDB.Impl;

namespace LevelDB.Util
{
    public class LevelIterator : AbstractSeekingIterator<InternalKey, Slice>, IInternalIterator
    {
        private readonly TableCache _tableCache;
        private readonly IList<FileMetaData> _files;
        private readonly InternalKeyComparator _comparator;
        private InternalTableIterator _current;
        private int _index;

        public LevelIterator(TableCache tableCache, IList<FileMetaData> files, InternalKeyComparator comparator)
        {
            _tableCache = tableCache;
            _files = files;
            _comparator = comparator;
        }

        protected override void SeekToFirstInternal()
        {
            // reset index to before first and clear the data iterator
            _index = 0;
            _current = null;
        }

        protected override void SeekInternal(InternalKey targetKey)
        {
            // seek the index to the block containing the key
            if (_files.Count == 0)
            {
                return;
            }

            // todo replace with Collections.binarySearch
            var left = 0;
            var right = _files.Count - 1;

            // binary search restart positions to find the restart position immediately before the targetKey
            while (left < right)
            {
                var mid = (left + right) / 2;

                if (_comparator.Compare(_files[mid].Largest, targetKey) < 0)
                {
                    // Key at "mid.largest" is < "target".  Therefore all
                    // files at or before "mid" are uninteresting.
                    left = mid + 1;
                }
                else
                {
                    // Key at "mid.largest" is >= "target".  Therefore all files
                    // after "mid" are uninteresting.
                    right = mid;
                }
            }
            _index = right;

            // if the index is now pointing to the last block in the file, check if the largest key
            // in the block is than the the target key.  If so, we need to seek beyond the end of this file
            if (_index == _files.Count - 1 && _comparator.Compare(_files[_index].Largest, targetKey) < 0)
            {
                _index++;
            }

            // if indexIterator does not have a next, it mean the key does not exist in this iterator
            if (_index < _files.Count)
            {
                // seek the current iterator to the key
                _current = OpenNextFile();
                _current.Seek(targetKey);
            }
            else
            {
                _current = null;
            }
        }

        protected override Entry<InternalKey, Slice> GetNextElement()
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
                    if (_index < _files.Count)
                    {
                        _current = OpenNextFile();
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

        private InternalTableIterator OpenNextFile()
        {
            var fileMetaData = _files[_index];
            _index++;
            return _tableCache.NewIterator(fileMetaData);
        }

        public override string ToString()
        {
            return $"ConcatenatingIterator(index={_index}, files={_files}, current={_current})";
        }
    }
}