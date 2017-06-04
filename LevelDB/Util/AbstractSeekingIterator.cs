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
using LevelDB.Impl;

namespace LevelDB.Util
{
    public abstract class AbstractSeekingIterator<TK, TV> : ISeekingIterator<TK, TV>
    {
        object IEnumerator.Current => Current;

        private Entry<TK, TV> _nextElement;

        public void SeekToFirst()
        {
            _nextElement = null;
            SeekToFirstInternal();
        }

        public void Seek(TK targetKey)
        {
            _nextElement = null;
            SeekInternal(targetKey);
        }

        public bool HasNext()
        {
            if (_nextElement == null)
            {
                _nextElement = GetNextElement();
            }
            return _nextElement != null;
        }

        public Entry<TK, TV> Next()
        {
            if (_nextElement == null)
            {
                _nextElement = GetNextElement();
                if (_nextElement == null)
                {
                    throw new InvalidOperationException();
                }
            }

            var result = _nextElement;
            _nextElement = null;
            return result;
        }

        public Entry<TK, TV> Peek()
        {
            if (_nextElement != null) return _nextElement;
            _nextElement = GetNextElement();
            if (_nextElement == null)
            {
                throw new InvalidOperationException();
            }

            return _nextElement;
        }

        public void Dispose()
        {
        }

        protected abstract void SeekToFirstInternal();

        protected abstract void SeekInternal(TK targetKey);

        protected abstract Entry<TK, TV> GetNextElement();

        #region UnSupported methods

        public Entry<TK, TV> Current => throw new NotSupportedException();

        public bool MoveNext()
        {
            throw new NotSupportedException();
        }

        public Entry<TK, TV> Remove()
        {
            throw new NotSupportedException();
        }

        public void Reset()
        {
            throw new NotSupportedException();
        }

        #endregion
    }
}