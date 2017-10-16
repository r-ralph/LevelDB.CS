using System;
using System.Collections.Generic;

namespace LevelDB.Guava
{
    public interface IPeekingIterator<out TE> : IEnumerator<TE>
    {
        TE Peek();
    }
}