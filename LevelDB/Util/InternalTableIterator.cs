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

using LevelDB.Impl;

namespace LevelDB.Util
{
    public class InternalTableIterator : AbstractSeekingIterator<InternalKey, Slice>, IInternalIterator
    {
        private readonly TableIterator _tableIterator;

        public InternalTableIterator(TableIterator tableIterator)
        {
            _tableIterator = tableIterator;
        }

        protected override void SeekToFirstInternal()
        {
            _tableIterator.SeekToFirst();
        }

        protected override void SeekInternal(InternalKey targetKey)
        {
            _tableIterator.Seek(targetKey.Encode());
        }

        protected override Entry<InternalKey, Slice> GetNextElement()
        {
            if (!_tableIterator.HasNext()) return null;
            var next = _tableIterator.Next();
            return new ImmutableEntry<InternalKey, Slice>(new InternalKey(next.Key), next.Value);
        }

        public override string ToString()
        {
            return $"InternalTableIterator(fromIterator={_tableIterator})";
        }
    }
}