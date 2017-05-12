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

using LevelDB.Guava;
using LevelDB.Table;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class InternalUserComparator : IUserComparator
    {
        private readonly InternalKeyComparator _internalKeyComparator;

        public InternalUserComparator(InternalKeyComparator internalKeyComparator)
        {
            _internalKeyComparator = internalKeyComparator;
        }


        public int Compare(Slice x, Slice y)
        {
            return _internalKeyComparator.Compare(new InternalKey(x), new InternalKey(y));
        }

        public string Name()
        {
            return _internalKeyComparator.Name;
        }

        public Slice FindShortestSeparator(Slice start, Slice limit)
        {
            // Attempt to shorten the user portion of the key
            var startUserKey = new InternalKey(start).UserKey;
            var limitUserKey = new InternalKey(limit).UserKey;

            var shortestSeparator =
                _internalKeyComparator.UserComparator.FindShortestSeparator(startUserKey, limitUserKey);

            if (_internalKeyComparator.UserComparator.Compare(startUserKey, shortestSeparator) >= 0)
            {
                return start;
            }
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            var newInternalKey = new InternalKey(shortestSeparator, SequenceNumber.MaxSequenceNumber, ValueType.Value);
            Preconditions.CheckState(Compare(start, newInternalKey.Encode()) < 0); // todo
            Preconditions.CheckState(Compare(newInternalKey.Encode(), limit) < 0); // todo
            return newInternalKey.Encode();
        }

        public Slice FindShortSuccessor(Slice key)
        {
            var userKey = new InternalKey(key).UserKey;
            var shortSuccessor = _internalKeyComparator.UserComparator.FindShortSuccessor(userKey);

            if (_internalKeyComparator.UserComparator.Compare(userKey, shortSuccessor) >= 0)
            {
                return key;
            }
            // User key has become larger.  Tack on the earliest possible
            // number to the shortened user key.
            var newInternalKey = new InternalKey(shortSuccessor, SequenceNumber.MaxSequenceNumber, ValueType.Value);
            Preconditions.CheckState(Compare(key, newInternalKey.Encode()) < 0); // todo
            return newInternalKey.Encode();
        }
    }
}