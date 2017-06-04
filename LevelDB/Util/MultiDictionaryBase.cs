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

namespace LevelDB.Util
{
    public abstract class MultiDictionaryBase<TK, TV>
    {
        public abstract List<TV> this[TK key] { get; set; }

        public abstract Dictionary<TK, List<TV>>.KeyCollection Keys { get; }

        public abstract Dictionary<TK, List<TV>>.ValueCollection Values { get; }

        public abstract int Count { get; }

        public abstract void Add(TK key, TV value);

        public abstract void Add(TK key, params TV[] values);

        public abstract void Add(TK key, IEnumerable<TV> values);

        public abstract void AddAll(MultiDictionaryBase<TK, TV> elements);

        public abstract bool Remove(TK key, TV value);

        public abstract bool Remove(TK key);

        public abstract void Clear();

        public abstract bool Contains(TK key, TV value);

        public abstract bool ContainsKey(TK key);

        public abstract IEnumerator<KeyValuePair<TK, List<TV>>> GetEnumerator();

        public abstract MultiDictionaryBase<TK, TV> ToImmutable();
    }
}