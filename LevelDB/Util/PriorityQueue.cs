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
    public class PriorityQueue<T> where T : IComparable<T>
    {
        private readonly List<T> _heap;

        public PriorityQueue() : this(16)
        {
        }

        public PriorityQueue(int capacity)
        {
            _heap = new List<T>(capacity);
        }

        public void Enqueue(T item)
        {
            _heap.Add(item);
            var i = Count++;
            while (i > 0)
            {
                var p = (i - 1) >> 1;
                if (_heap[p].CompareTo(item) <= 0)
                    break;
                _heap[i] = _heap[p];
                i = p;
            }
            _heap[i] = item;
        }

        public T Dequeue()
        {
            var ret = _heap[0];
            var x = _heap[--Count];
            var i = 0;
            while ((i << 1) + 1 < Count)
            {
                var a = (i << 1) + 1;
                var b = (i << 1) + 2;
                if (b < Count && _heap[b].CompareTo(_heap[a]) < 0) a = b;
                if (_heap[a].CompareTo(x) >= 0)
                    break;
                _heap[i] = _heap[a];
                i = a;
            }
            _heap[i] = x;
            _heap.RemoveAt(Count);
            return ret;
        }

        public T Peek()
        {
            return _heap[0];
        }

        public int Count { get; private set; }

        public bool Any()
        {
            return Count > 0;
        }
    }
}