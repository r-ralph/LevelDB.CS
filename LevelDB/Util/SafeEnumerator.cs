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

using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace LevelDB.Util
{
    /**
     * https://stackoverflow.com/questions/22308067/thread-safe-sorteddictionary
     */
    public class SafeEnumerator<T> : IEnumerator<T>
    {
        #region Variables

        // this is the (thread-unsafe)
        // enumerator of the underlying collection
        private readonly IEnumerator<T> _enumerator;

        // this is the object we shall lock on. 
        private ReaderWriterLockSlim _lock;

        #endregion

        #region Constructor

        public SafeEnumerator(IEnumerator<T> inner, ReaderWriterLockSlim readWriteLock)
        {
            _enumerator = inner;
            _lock = readWriteLock;

            // Enter lock in constructor
            _lock.EnterReadLock();
        }

        #endregion

        #region Implementation of IDisposable

        public void Dispose()
        {
            // .. and exiting lock on Dispose()
            // This will be called when the foreach loop finishes
            _lock.ExitReadLock();
        }

        #endregion

        #region Implementation of IEnumerator

        // we just delegate actual implementation
        // to the inner enumerator, that actually iterates
        // over some collection

        public bool MoveNext()
        {
            return _enumerator.MoveNext();
        }

        public void Reset()
        {
            _enumerator.Reset();
        }

        public T Current
        {
            get { return _enumerator.Current; }
        }

        object IEnumerator.Current
        {
            get { return Current; }
        }

        #endregion
    }
}