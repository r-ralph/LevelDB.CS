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
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace LevelDB.Util
{
    /**
     * https://stackoverflow.com/questions/22308067/thread-safe-sorteddictionary
     */
    public class ConcurrentSortedDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        #region Variables

        private readonly ReaderWriterLockSlim _readWriteLock = new ReaderWriterLockSlim();
        readonly SortedDictionary<TKey, TValue> _dict;

        #endregion

        #region Constructors

        public ConcurrentSortedDictionary()
        {
            _dict = new SortedDictionary<TKey, TValue>();
        }

        public ConcurrentSortedDictionary(IComparer<TKey> comparer)
        {
            _dict = new SortedDictionary<TKey, TValue>(comparer);
        }

        public ConcurrentSortedDictionary(IDictionary<TKey, TValue> dictionary)
        {
            _dict = new SortedDictionary<TKey, TValue>(dictionary);
        }

        public ConcurrentSortedDictionary(IDictionary<TKey, TValue> dictionary, IComparer<TKey> comparer)
        {
            _dict = new SortedDictionary<TKey, TValue>(dictionary, comparer);
        }

        #endregion

        #region Properties

        public IComparer<TKey> Comparer
        {
            get
            {
                _readWriteLock.EnterReadLock();
                try
                {
                    return _dict.Comparer;
                }
                finally
                {
                    _readWriteLock.ExitReadLock();
                }
            }
        }

        public int Count
        {
            get
            {
                _readWriteLock.EnterReadLock();
                try
                {
                    return _dict.Count;
                }
                finally
                {
                    _readWriteLock.ExitReadLock();
                }
            }
        }

        public TValue this[TKey key]
        {
            get
            {
                _readWriteLock.EnterReadLock();
                try
                {
                    return _dict[key];
                }
                finally
                {
                    _readWriteLock.ExitReadLock();
                }
            }
            set
            {
                _readWriteLock.EnterWriteLock();
                try
                {
                    _dict[key] = value;
                }
                finally
                {
                    _readWriteLock.ExitWriteLock();
                }
            }
        }

        public SortedDictionary<TKey, TValue>.KeyCollection Keys
        {
            get
            {
                _readWriteLock.EnterReadLock();
                try
                {
                    return new SortedDictionary<TKey, TValue>.KeyCollection(_dict);
                }
                finally
                {
                    _readWriteLock.ExitReadLock();
                }
            }
        }

        public SortedDictionary<TKey, TValue>.ValueCollection Values
        {
            get
            {
                _readWriteLock.EnterReadLock();
                try
                {
                    return new SortedDictionary<TKey, TValue>.ValueCollection(_dict);
                }
                finally
                {
                    _readWriteLock.ExitReadLock();
                }
            }
        }
        public bool IsEmpty => Count == 0;

        #endregion

        #region Methods

        public void Add(TKey key, TValue value)
        {
            _readWriteLock.EnterWriteLock();
            try
            {
                _dict.Add(key, value);
            }
            finally
            {
                _readWriteLock.ExitWriteLock();
            }
        }

        public void Clear()
        {
            _readWriteLock.EnterWriteLock();
            try
            {
                _dict.Clear();
            }
            finally
            {
                _readWriteLock.ExitWriteLock();
            }
        }

        public bool ContainsKey(TKey key)
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.ContainsKey(key);
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public bool ContainsValue(TValue value)
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.ContainsValue(value);
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int index)
        {
            _readWriteLock.EnterReadLock();
            try
            {
                _dict.CopyTo(array, index);
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public override bool Equals(Object obj)
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.Equals(obj);
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new SafeEnumerator<KeyValuePair<TKey, TValue>>(_dict.GetEnumerator(), _readWriteLock);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new SafeEnumerator<KeyValuePair<TKey, TValue>>(_dict.GetEnumerator(), _readWriteLock);
        }

        public override int GetHashCode()
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.GetHashCode();
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public bool Remove(TKey key)
        {
            _readWriteLock.EnterWriteLock();
            try
            {
                return _dict.Remove(key);
            }
            finally
            {
                _readWriteLock.ExitWriteLock();
            }
        }

        public override string ToString()
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.ToString();
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            _readWriteLock.EnterReadLock();
            try
            {
                return _dict.TryGetValue(key, out value);
            }
            finally
            {
                _readWriteLock.ExitReadLock();
            }
        }

        #endregion
    }
}