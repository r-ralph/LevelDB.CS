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
using LevelDB.Util;

namespace LevelDB.Impl
{
    public sealed class SnapshotSeekingIterator : AbstractSeekingIterator<Slice, Slice>
    {
        private readonly DbIterator _iterator;
        private readonly SnapshotImpl _snapshot;
        private readonly IComparer<Slice> _userComparator;

        public SnapshotSeekingIterator(DbIterator iterator, SnapshotImpl snapshot, IComparer<Slice> userComparator)
        {
            _iterator = iterator;
            _snapshot = snapshot;
            _userComparator = userComparator;
            _snapshot.GetVersion().Retain();
        }

        public void Close()
        {
            _snapshot.GetVersion().Release();
        }

        protected override void SeekToFirstInternal()
        {
            _iterator.SeekToFirst();
            FindNextUserEntry(null);
        }


        protected override void SeekInternal(Slice targetKey)
        {
            _iterator.Seek(new InternalKey(targetKey, _snapshot.GetLastSequence(), ValueType.Value));
            FindNextUserEntry(null);
        }

        protected override Entry<Slice, Slice> GetNextElement()
        {
            if (!_iterator.HasNext())
            {
                return null;
            }

            var next = _iterator.Next();

            // find the next user entry after the key we are about to return
            FindNextUserEntry(next.Key.UserKey);

            return new ImmutableEntry<Slice, Slice>(next.Key.UserKey, next.Value);
        }

        private void FindNextUserEntry(Slice deletedKey)
        {
            // if there are no more entries, we are done
            if (!_iterator.HasNext())
            {
                return;
            }
            do
            {
                // Peek the next entry and parse the key
                var internalKey = _iterator.Peek().Key;

                // skip entries created after our snapshot
                if (internalKey.SequenceNumber > _snapshot.GetLastSequence())
                {
                    _iterator.Next();
                    continue;
                }

                // if the next entry is a deletion, skip all subsequent entries for that key
                if (internalKey.ValueType == ValueType.Deletion)
                {
                    deletedKey = internalKey.UserKey;
                }
                else if (internalKey.ValueType == ValueType.Value)
                {
                    // is this value masked by a prior deletion record?
                    if (deletedKey == null || _userComparator.Compare(internalKey.UserKey, deletedKey) > 0)
                    {
                        return;
                    }
                }
                _iterator.Next();
            } while (_iterator.HasNext());
        }

        public override string ToString()
        {
            return $"SnapshotSeekingIterator(snapshot={_snapshot}, iterator={_iterator})";
        }
    }
}