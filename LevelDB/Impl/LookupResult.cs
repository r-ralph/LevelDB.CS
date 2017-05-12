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

using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class LookupResult
    {
        public static LookupResult Ok(LookupKey key, Slice value)
        {
            return new LookupResult(key, value, false);
        }

        public static LookupResult Deleted(LookupKey key)
        {
            return new LookupResult(key, null, true);
        }

        public LookupKey Key { get; }
        public Slice Value { get; }
        public bool IsDeleted { get; }

        private LookupResult(LookupKey key, Slice value, bool deleted)
        {
            Preconditions.CheckNotNull(key, $"{key} is null");
            Key = key;
            Value = value?.Sliced();
            IsDeleted = deleted;
        }
    }
}