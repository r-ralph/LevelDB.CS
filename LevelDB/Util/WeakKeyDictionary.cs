// Copyright 2004-2011 Castle Project - http://www.castleproject.org/
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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace LevelDB.Util
{
    public class WeakKeyDictionary<TKey, TValue> : IDictionary<TKey, TValue> where TKey : class
    {
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;

        public ICollection<TKey> Keys => _keys ?? (_keys = new KeyCollection(_dictionary.Keys));

        public ICollection<TValue> Values => _dictionary.Values;

        public TValue this[TKey key]
        {
            get
            {
                Age(1);
                return _dictionary[key];
            }
            set
            {
                Age(4);
                _dictionary[_comparer.Wrap(key)] = value;
            }
        }

        private readonly Dictionary<object, TValue> _dictionary;
        private readonly WeakKeyComparer<TKey> _comparer;
        private KeyCollection _keys;

        private int _age; // Incremented by operations
        private const int AgeThreshold = 128; // Age at which to trim dead objects

        public WeakKeyDictionary()
            : this(0, EqualityComparer<TKey>.Default)
        {
        }

        public WeakKeyDictionary(int capacity)
            : this(capacity, EqualityComparer<TKey>.Default)
        {
        }

        public WeakKeyDictionary(IEqualityComparer<TKey> comparer)
            : this(0, comparer)
        {
        }

        public WeakKeyDictionary(int capacity, IEqualityComparer<TKey> comparer)
        {
            _comparer = new WeakKeyComparer<TKey>(comparer);
            _dictionary = new Dictionary<object, TValue>(capacity, _comparer);
        }

        public int Count
        {
            get
            {
                Age(1);
                return _dictionary.Count;
            }
        }

        public bool ContainsKey(TKey key)
        {
            Age(1);
            return _dictionary.ContainsKey(key);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            Age(1);
            TValue candidate;
            return _dictionary.TryGetValue(item.Key, out candidate)
                   && EqualityComparer<TValue>.Default.Equals(candidate, item.Value);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            Age(1);
            return _dictionary.TryGetValue(key, out value);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var hasDeadObjects = false;

            foreach (var wrapped in _dictionary)
            {
                var item = new KeyValuePair<TKey, TValue>
                (
                    _comparer.Unwrap(wrapped.Key),
                    wrapped.Value
                );

                if (item.Key == null)
                    hasDeadObjects = true;
                else
                    yield return item;
            }

            if (hasDeadObjects)
                TrimDeadObjects();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            foreach (var item in this)
                array[index++] = item;
        }

        public void Add(TKey key, TValue value)
        {
            Age(2);
            _dictionary.Add(_comparer.Wrap(key), value);
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        public bool Remove(TKey key)
        {
            Age(4);
            return _dictionary.Remove(key);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            ICollection<KeyValuePair<TKey, TValue>> collection = this;
            return collection.Contains(item) && Remove(item.Key);
        }

        public void Clear()
        {
            _age = 0;
            _dictionary.Clear();
        }

        private void Age(int amount)
        {
            if ((_age += amount) > AgeThreshold)
                TrimDeadObjects();
        }

        public void TrimDeadObjects()
        {
            _age = 0;
            var removals = null as List<object>;

            foreach (var key in _dictionary.Keys)
            {
                if (_comparer.Unwrap(key) == null)
                {
                    if (removals == null)
                        removals = new List<object>();
                    removals.Add(key);
                }
            }

            if (removals == null) return;
            foreach (var key in removals)
            {
                _dictionary.Remove(key);
            }
        }

        private class KeyCollection : ICollection<TKey>
        {
            private readonly ICollection<object> _keys;

            public KeyCollection(ICollection<object> keys)
            {
                _keys = keys;
            }

            public int Count => _keys.Count;

            bool ICollection<TKey>.IsReadOnly => true;

            public bool Contains(TKey item)
            {
                return _keys.Contains(item);
            }

            public IEnumerator<TKey> GetEnumerator()
            {
                return _keys.Select(key => (TKey) ((WeakKey) key).Target)
                    .Where(target => target != null)
                    .GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            public void CopyTo(TKey[] array, int index)
            {
                foreach (var key in this)
                    array[index++] = key;
            }

            void ICollection<TKey>.Add(TKey item)
            {
                throw ReadOnlyCollectionError();
            }

            bool ICollection<TKey>.Remove(TKey item)
            {
                throw ReadOnlyCollectionError();
            }

            void ICollection<TKey>.Clear()
            {
                throw ReadOnlyCollectionError();
            }

            private static Exception ReadOnlyCollectionError()
            {
                return new NotSupportedException("The collection is read-only.");
            }
        }
    }
}