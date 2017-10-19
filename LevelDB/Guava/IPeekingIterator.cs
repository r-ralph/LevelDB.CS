using System;
using System.Collections.Generic;

namespace LevelDB.Guava
{
    /// <summary>
    /// An iterator that supports a one-element lookahead while iterating.
    /// Porting class from Google Guava
    /// </summary>
    public interface IPeekingIterator<out TE> : IEnumerator<TE>
    {
        /// <summary>
        /// Returns the next element in the iteration, without advancing the iteration. Calls to <code>peek()</code>
        /// should not change the state of the iteration, except that it may prevent removal of the most recent
        /// element via remove().
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">If the iteration has no more elements according to HasNext()</exception>
        TE Peek();

        /// <summary>
        /// The objects returned by consecutive calls to peek() then next() are guaranteed to be equal to each other.
        /// </summary>
        /// <returns></returns>
        TE Next();

        /// <summary>
        /// Implementations may or may not support removal when a call to peek() has occurred since the most recent call to next().
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">If there has been a call to peek() since the most recent call
        /// to next() and this implementation does not support this sequence of calls (optional)</exception>
        TE Remove();

        /// <summary>
        /// Return this iterator is reached ended or not.
        /// </summary>
        /// <returns></returns>
        bool HasNext();
    }
}