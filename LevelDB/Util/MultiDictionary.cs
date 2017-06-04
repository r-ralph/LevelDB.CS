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
    public class MultiDictionary<TKey, TValue> : MultiDictionaryBase<TKey, TValue>
    {
        private readonly Dictionary<TKey, List<TValue>> _mDictionary = new Dictionary<TKey, List<TValue>>();

        public override List<TValue> this[TKey key]
        {
            get => _mDictionary[key];
            set => _mDictionary[key] = value;
        }

        public override Dictionary<TKey, List<TValue>>.KeyCollection Keys => _mDictionary.Keys;

        public override Dictionary<TKey, List<TValue>>.ValueCollection Values => _mDictionary.Values;

        public override int Count => _mDictionary.Count;

        public override void Add(TKey key, TValue value)
        {
            if (!_mDictionary.ContainsKey(key))
            {
                _mDictionary.Add(key, new List<TValue>());
            }
            _mDictionary[key].Add(value);
        }

        public override void Add(TKey key, params TValue[] values)
        {
            foreach (var n in values)
            {
                Add(key, n);
            }
        }

        public override void Add(TKey key, IEnumerable<TValue> values)
        {
            foreach (var n in values)
            {
                Add(key, n);
            }
        }

        public override void AddAll(MultiDictionaryBase<TKey, TValue> elements)
        {
            foreach (var entry in elements)
            {
                Add(entry.Key, entry.Value);
            }
        }

        public override bool Remove(TKey key, TValue value)
        {
            return _mDictionary[key].Remove(value);
        }

        public override bool Remove(TKey key)
        {
            return _mDictionary.Remove(key);
        }

        public override void Clear()
        {
            _mDictionary.Clear();
        }

        public override bool Contains(TKey key, TValue value)
        {
            return _mDictionary[key].Contains(value);
        }

        public override bool ContainsKey(TKey key)
        {
            return _mDictionary.ContainsKey(key);
        }

        public override IEnumerator<KeyValuePair<TKey, List<TValue>>> GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<TKey, List<TValue>>>) _mDictionary).GetEnumerator();
        }

        public override MultiDictionaryBase<TKey, TValue> ToImmutable()
        {
            return new ImmutableMultiDictionary<TKey, TValue>(this);
        }
    }
}