using System;
using System.Collections;
using System.Collections.Generic;
using LevelDB.Impl;
using LevelDB.Util;

namespace LevelDB.Guava
{
    public class Iterators
    {
        private class PeekingIteratorImpl<TK, TV> : IPeekingIterator<Entry<TK, TV>>
        {
            private readonly IEnumerator<KeyValuePair<TK, TV>> _iterator;
            private bool _hasNext;
            private Entry<TK, TV> _current;

            public PeekingIteratorImpl(IEnumerator<KeyValuePair<TK, TV>> iterator)
            {
                _iterator = Preconditions.CheckNotNull(iterator);
                if (_iterator.MoveNext())
                {
                    _current = new ImmutableEntry<TK, TV>(_iterator.Current.Key, _iterator.Current.Value);
                }
                _hasNext = _iterator.MoveNext();
            }

            object IEnumerator.Current => Current;

            public bool HasNext()
            {
                return _hasNext;
            }

            public Entry<TK, TV> Next()
            {
                var ret = _current;
                if (_hasNext)
                {
                    _current = new ImmutableEntry<TK, TV>(_iterator.Current.Key, _iterator.Current.Value);
                }
                _hasNext = _iterator.MoveNext();
                return ret;
            }

            public Entry<TK, TV> Peek()
            {
                return _current;
            }

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

            public void Dispose()
            {
                throw new NotSupportedException();
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            #endregion
        }

        /// <summary>
        /// Returns a {@code PeekingIterator} backed by the given iterator.
        ///
        /// <p>Calls to the {@code peek} method with no intervening calls to {@code
        /// next} do not affect the iteration, and hence return the same object each
        /// time. A subsequent call to {@code next} is guaranteed to return the same
        /// object again. For example: <pre>   {@code
        ///
        ///   PeekingIterator<String> peekingIterator =
        ///       Iterators.peekingIterator(Iterators.forArray("a", "b"));
        ///   String a1 = peekingIterator.peek(); // returns "a"
        ///   String a2 = peekingIterator.peek(); // also returns "a"
        ///   String a3 = peekingIterator.next(); // also returns "a"}</pre>
        ///
        /// <p>Any structural changes to the underlying iteration (aside from those
        /// performed by the iterator's own {@link PeekingIterator#remove()} method)
        /// will leave the iterator in an undefined state.
        ///
        /// <p>The returned iterator does not support removal after peeking, as
        /// explained by {@link PeekingIterator#remove()}.
        ///
        /// <p>Note: If the given iterator is already a {@code PeekingIterator},
        /// it <i>might</i> be returned to the caller, although this is neither
        /// guaranteed to occur nor required to be consistent.  For example, this
        /// method <i>might</i> choose to pass through recognized implementations of
        /// {@code PeekingIterator} when the behavior of the implementation is
        /// known to meet the contract guaranteed by this method.
        ///
        /// <p>There is no {@link Iterable} equivalent to this method, so use this
        /// method to wrap each individual iterator as it is generated.
        ///
        /// @param iterator the backing iterator. The {@link PeekingIterator} assumes
        ///     ownership of this iterator, so users should cease making direct calls
        ///     to it after calling this method.
        /// @return a peeking iterator backed by that iterator. Apart from the
        ///     additional {@link PeekingIterator#peek()} method, this iterator behaves
        ///     exactly the same as {@code iterator}.
        /// </summary>
        public static IPeekingIterator<Entry<TK, TV>> PeekingIterator<TK, TV>(
            IEnumerator<KeyValuePair<TK, TV>> iterator)
        {
            return new PeekingIteratorImpl<TK, TV>(iterator);
        }
    }
}