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
using LevelDB.Util;

namespace LevelDB.Table
{
    public class BytewiseComparator : IUserComparator
    {
        public string Name()
        {
            return "leveldb.BytewiseComparator";
        }

        public int Compare(Slice x, Slice y)
        {
            return x?.CompareTo(y) ?? 0;
        }

        public Slice FindShortestSeparator(Slice start, Slice limit)
        {
            // Find length of common prefix
            var sharedBytes = BlockBuilder.CalculateSharedBytes(start, limit);

            // Do not shorten if one string is a prefix of the other
            if (sharedBytes >= Math.Min(start.Length, limit.Length)) return start;
            // if we can add one to the last shared byte without overflow and the two keys differ by more than
            // one increment at this location.
            var lastSharedByte = start.GetByte(sharedBytes);
            if (lastSharedByte >= 0xff || lastSharedByte + 1 >= limit.GetByte(sharedBytes)) return start;
            var result = start.CopySlice(0, sharedBytes + 1);
            result.SetByte(sharedBytes, (byte) (lastSharedByte + 1));

            return result;
        }

        public Slice FindShortSuccessor(Slice key)
        {
            // Find first character that can be incremented
            for (var i = 0; i < key.Length; i++)
            {
                var b = key.GetByte(i);
                if (b == 0xff) continue;
                var result = key.CopySlice(0, i + 1);
                result.SetByte(i, (byte) (b + 1));
                return result;
            }
            // key is a run of 0xffs.  Leave it alone.
            return key;
        }
    }
}