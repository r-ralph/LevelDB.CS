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
using LevelDB.Guava;

namespace LevelDB.Util
{
    public class IntVector
    {
        private int[] _values;

        public int Size { get; private set; }

        public int[] Values
        {
            get
            {
                var retValues = new int[Size];
                Array.Copy(_values, retValues, Size);
                return retValues;
            }
        }

        public IntVector(int initialCapacity)
        {
            _values = new int[initialCapacity];
        }


        public void Clear()
        {
            Size = 0;
        }

        public void Add(int value)
        {
            Preconditions.CheckArgument(Size + 1 >= 0, "Invalid minLength: %s", Size + 1);

            EnsureCapacity(Size + 1);

            _values[Size++] = value;
        }

        private void EnsureCapacity(int minCapacity)
        {
            if (_values.Length >= minCapacity)
            {
                return;
            }

            var newLength = _values.Length;
            if (newLength == 0)
            {
                newLength = 1;
            }
            else
            {
                newLength <<= 1;
            }
            var newValues = new int[newLength];
            Array.Copy(_values, newValues, _values.Length);
            _values = newValues;
        }

        public void Write(SliceOutput sliceOutput)
        {
            for (var index = 0; index < Size; index++)
            {
                sliceOutput.WriteInt(_values[index]);
            }
        }

        public override string ToString()
        {
            return $"IntVector(size={Size}, values={Convert.ToString(_values)})";
        }
    }
}