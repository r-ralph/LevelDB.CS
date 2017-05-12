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
using System.Linq;
using LevelDB.Guava;
using LevelDB.Table;
using LevelDB.Util.Extension;

namespace LevelDB.Impl
{
    public class InternalKeyComparator : Comparer<InternalKey>
    {
        public IUserComparator UserComparator { get; }

        public string Name => UserComparator.Name();

        public InternalKeyComparator(IUserComparator userComparator)
        {
            UserComparator = userComparator;
        }

        public override int Compare(InternalKey left, InternalKey right)
        {
            if (left == null || right == null)
            {
                return 0;
            }
            var result = UserComparator.Compare(left.UserKey, right.UserKey);
            return result != 0 ? result : Primitives.Compare(right.SequenceNumber, left.SequenceNumber);
        }

        /// <summary>
        /// Returns {@code true} if each element in {@code iterable} after the first is
        /// greater than or equal to the element that preceded it, according to this
        /// ordering. Note that this is always true when the iterable has fewer than
        /// two elements.
        /// </summary>
        public bool IsOrdered(params InternalKey[] keys)
        {
            return keys.ToList().IsOrdered(this);
        }
    }
}