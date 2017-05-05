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

namespace LevelDB
{
    public class Range
    {
        private readonly byte[] _start;
        private readonly byte[] _limit;

        public byte[] Limit()
        {
            return _limit;
        }

        public byte[] Start()
        {
            return _start;
        }

        public Range(byte[] start, byte[] limit)
        {
            Options.CheckArgNotNull(start, "start");
            Options.CheckArgNotNull(limit, "limit");
            _limit = limit;
            _start = start;
        }
    }
}