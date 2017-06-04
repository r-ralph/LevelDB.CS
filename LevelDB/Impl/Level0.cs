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
using System.Linq;
using System.Text;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class Level0 : ISeekingIterable<InternalKey, Slice>
    {
        public int LevelNumber => 0;
        public List<FileMetaData> Files { get; }

        private readonly TableCache _tableCache;
        private readonly InternalKeyComparator _internalKeyComparator;

        public class NewestFirstComparer : IComparer<FileMetaData>
        {
            public static readonly NewestFirstComparer Instance = new NewestFirstComparer();

            private NewestFirstComparer()
            {
            }

            public int Compare(FileMetaData fileMetaData, FileMetaData fileMetaData1)
            {
                if (fileMetaData1 != null && fileMetaData != null)
                {
                    return (int) (fileMetaData1.Number - fileMetaData.Number);
                }
                return 0;
            }
        }

        public Level0(List<FileMetaData> files, TableCache tableCache, InternalKeyComparator internalKeyComparator)
        {
            Preconditions.CheckNotNull(files, "files is null");
            Preconditions.CheckNotNull(tableCache, "tableCache is null");
            Preconditions.CheckNotNull(internalKeyComparator, "internalKeyComparator is null");

            Files = new List<FileMetaData>(files);
            _tableCache = tableCache;
            _internalKeyComparator = internalKeyComparator;
        }

        public Level0Iterator GetLevel0Iterator()
        {
            return new Level0Iterator(_tableCache, Files, _internalKeyComparator);
        }

        public IEnumerator<Entry<InternalKey, Slice>> GetEnumerator()
        {
            return GetLevel0Iterator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetLevel0Iterator();
        }

        public LookupResult Get(LookupKey key, ReadStats readStats)
        {
            if (Files.Count == 0)
            {
                return null;
            }

            var fileMetaDataList = new List<FileMetaData>(Files.Count);
            fileMetaDataList.AddRange(Files.Where(fileMetaData =>
                _internalKeyComparator.UserComparator.Compare(key.UserKey, fileMetaData.Smallest.UserKey) >= 0 &&
                _internalKeyComparator.UserComparator.Compare(key.UserKey, fileMetaData.Largest.UserKey) <= 0));

            fileMetaDataList.Sort(NewestFirstComparer.Instance);

            readStats.Clear();
            foreach (var fileMetaData in fileMetaDataList)
            {
                // open the iterator
                var iterator = _tableCache.NewIterator(fileMetaData);

                // seek to the key
                iterator.Seek(key.InternalKey);

                if (iterator.HasNext())
                {
                    // parse the key in the block
                    var entry = iterator.Next();
                    var internalKey = entry.Key;
                    Preconditions.CheckState(internalKey != null,
                        $"Corrupt key for {key.UserKey.ToString(Encoding.UTF8)}");

                    // if this is a value key (not a delete) and the keys match, return the value
                    // ReSharper disable once PossibleNullReferenceException
                    if (key.UserKey.Equals(internalKey.UserKey))
                    {
                        if (internalKey.ValueType == ValueType.Deletion)
                        {
                            return LookupResult.Deleted(key);
                        }
                        if (internalKey.ValueType == ValueType.Value)
                        {
                            return LookupResult.Ok(key, entry.Value);
                        }
                    }
                }

                if (readStats.SeekFile == null)
                {
                    // We have had more than one seek for this read.  Charge the first file.
                    readStats.SeekFile = fileMetaData;
                    readStats.SeekFileLevel = 0;
                }
            }

            return null;
        }

        public bool SomeFileOverlapsRange(Slice smallestUserKey, Slice largestUserKey)
        {
            var smallestInternalKey =
                new InternalKey(smallestUserKey, SequenceNumber.MaxSequenceNumber, ValueType.Value);
            var index = FindFile(smallestInternalKey);

            var userComparator = _internalKeyComparator.UserComparator;
            return index < Files.Count && userComparator.Compare(largestUserKey, Files[index].Smallest.UserKey) >= 0;
        }

        private int FindFile(InternalKey targetKey)
        {
            if (Files.Count == 0)
            {
                return 0;
            }

            // todo replace with Collections.binarySearch
            var left = 0;
            var right = Files.Count - 1;

            // binary search restart positions to find the restart position immediately before the targetKey
            while (left < right)
            {
                var mid = (left + right) / 2;

                if (_internalKeyComparator.Compare(Files[mid].Largest, targetKey) < 0)
                {
                    // Key at "mid.largest" is < "target".  Therefore all
                    // files at or before "mid" are uninteresting.
                    left = mid + 1;
                }
                else
                {
                    // Key at "mid.largest" is >= "target".  Therefore all files
                    // after "mid" are uninteresting.
                    right = mid;
                }
            }
            return right;
        }

        public void AddFile(FileMetaData fileMetaData)
        {
            // todo remove mutation
            Files.Add(fileMetaData);
        }

        public override string ToString()
        {
            return $"Level0(files={Files})";
        }
    }
}