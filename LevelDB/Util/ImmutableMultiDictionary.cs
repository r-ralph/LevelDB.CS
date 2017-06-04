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

namespace LevelDB.Util
{
    public class ImmutableMultiDictionary<TK, TV> : MultiDictionaryBase<TK, TV>
    {
        private readonly MultiDictionaryBase<TK, TV> _parent;

        public ImmutableMultiDictionary(MultiDictionaryBase<TK, TV> parent)
        {
            _parent = parent;
        }

        public override List<TV> this[TK key]
        {
            get => _parent[key];
            set => _parent[key] = value;
        }

        public override Dictionary<TK, List<TV>>.KeyCollection Keys => _parent.Keys;
        public override Dictionary<TK, List<TV>>.ValueCollection Values => _parent.Values;
        public override int Count => _parent.Count;

        public override void Add(TK key, TV value)
        {
            throw new NotSupportedException();
        }

        public override void Add(TK key, params TV[] values)
        {
            throw new NotSupportedException();
        }

        public override void Add(TK key, IEnumerable<TV> values)
        {
            throw new NotSupportedException();
        }

        public override void AddAll(MultiDictionaryBase<TK, TV> elements)
        {
            throw new NotSupportedException();
        }

        public override bool Remove(TK key, TV value)
        {
            throw new NotSupportedException();
        }

        public override bool Remove(TK key)
        {
            throw new NotSupportedException();
        }

        public override void Clear()
        {
            throw new NotSupportedException();
        }

        public override bool Contains(TK key, TV value)
        {
            return _parent.Contains(key, value);
        }

        public override bool ContainsKey(TK key)
        {
            return _parent.ContainsKey(key);
        }

        public override IEnumerator<KeyValuePair<TK, List<TV>>> GetEnumerator()
        {
            return _parent.GetEnumerator();
        }

        public override MultiDictionaryBase<TK, TV> ToImmutable()
        {
            return this;
        }
    }
}