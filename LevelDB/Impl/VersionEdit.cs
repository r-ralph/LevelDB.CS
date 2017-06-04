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

using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class VersionEdit
    {
        public string ComparatorName { get; set; }
        public long? LogNumber { get; set; }
        public long NextFileNumber { get; set; }
        public long? PreviousLogNumber { get; set; }
        public long LastSequenceNumber { get; set; }
        public MultiDictionaryBase<int, FileMetaData> NewFiles => _newFiles.ToImmutable();
        public MultiDictionaryBase<int, long> DeletedFiles => _deletedFiles.ToImmutable();

        private readonly SortedDictionary<int, InternalKey> _compactPointers = new SortedDictionary<int, InternalKey>();
        private readonly MultiDictionary<int, FileMetaData> _newFiles = new MultiDictionary<int, FileMetaData>();
        private readonly MultiDictionary<int, long> _deletedFiles = new MultiDictionary<int, long>();

        public VersionEdit()
        {
        }

        public VersionEdit(Slice slice)
        {
            var sliceInput = slice.Input();
            while (sliceInput.CanRead)
            {
                var i = VariableLengthQuantity.ReadVariableLengthInt(sliceInput);
                var tag = VersionEditTag.GetValueTypeByPersistentId((int) i);
                tag.ReadValue(sliceInput, this);
            }
        }

        public IDictionary<int, InternalKey> GetCompactPointers()
        {
            return _compactPointers.ToImmutableDictionary();
        }

        public void SetCompactPointer(int level, InternalKey key)
        {
            _compactPointers[level] = key;
        }

        public void SetCompactPointers(IDictionary<int, InternalKey> compactPointers)
        {
            foreach (var entry in compactPointers)
            {
                _compactPointers[entry.Key] = entry.Value;
            }
        }

        /// <summary>
        /// Add the specified file at the specified level.
        /// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
        /// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
        /// </summary>
        /// <param name="level"></param>
        /// <param name="fileNumber"></param>
        /// <param name="fileSize"></param>
        /// <param name="smallest"></param>
        /// <param name="largest"></param>
        public void AddFile(int level, long fileNumber,
            long fileSize,
            InternalKey smallest,
            InternalKey largest)
        {
            var fileMetaData = new FileMetaData(fileNumber, fileSize, smallest, largest);
            AddFile(level, fileMetaData);
        }

        public void AddFile(int level, FileMetaData fileMetaData)
        {
            _newFiles.Add(level, fileMetaData);
        }

        public void AddFiles(MultiDictionaryBase<int, FileMetaData> files)
        {
            _newFiles.AddAll(files);
        }

        /// <summary>
        /// Delete the specified "file" from the specified "level".
        /// </summary>
        /// <param name="level"></param>
        /// <param name="fileNumber"></param>
        public void DeleteFile(int level, long fileNumber)
        {
            _deletedFiles.Add(level, fileNumber);
        }

        public Slice Encode()
        {
            var dynamicSliceOutput = new DynamicSliceOutput(4096);
            foreach (var versionEditTag in VersionEditTag.Values())
            {
                versionEditTag.WriteValue(dynamicSliceOutput, this);
            }
            return dynamicSliceOutput.Sliced();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append("VersionEdit");
            sb.Append("{comparatorName='").Append(ComparatorName).Append('\'');
            sb.Append(", logNumber=").Append(LogNumber);
            sb.Append(", previousLogNumber=").Append(PreviousLogNumber);
            sb.Append(", lastSequenceNumber=").Append(LastSequenceNumber);
            sb.Append(", compactPointers=").Append(_compactPointers);
            sb.Append(", newFiles=").Append(_newFiles);
            sb.Append(", deletedFiles=").Append(_deletedFiles);
            sb.Append('}');
            return sb.ToString();
        }
    }
}