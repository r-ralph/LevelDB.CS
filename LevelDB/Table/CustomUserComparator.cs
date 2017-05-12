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

using LevelDB.Util;

namespace LevelDB.Table
{
    public class CustomUserComparator : IUserComparator
    {
        private readonly IDBComparator _comparator;

        public CustomUserComparator(IDBComparator comparator)
        {
            _comparator = comparator;
        }

        public string Name()
        {
            return _comparator.Name();
        }

        public Slice FindShortestSeparator(Slice start, Slice limit)
        {
            return new Slice(_comparator.FindShortestSeparator(start.GetBytes(), limit.GetBytes()));
        }

        public Slice FindShortSuccessor(Slice key)
        {
            return new Slice(_comparator.FindShortSuccessor(key.GetBytes()));
        }

        public int Compare(Slice x, Slice y)
        {
            if (x != null && y != null) return _comparator.Compare(x.GetBytes(), y.GetBytes());
            return 0;
        }
    }
}