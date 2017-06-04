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
using System.Text;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public sealed class VersionEditTag
    {
        public static readonly VersionEditTag Comparator = new VersionEditTag(1,
            (sliceInput, versionEdit) =>
            {
                var bytes = new byte[VariableLengthQuantity.ReadVariableLengthInt(sliceInput)];
                sliceInput.ReadBytes(bytes);
                versionEdit.ComparatorName = Encoding.UTF8.GetString(bytes);
            },
            (sliceOutput, versionEdit, self) =>
            {
                var comparatorName = versionEdit.ComparatorName;
                if (comparatorName == null) return;
                VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                var bytes = Encoding.UTF8.GetBytes(comparatorName);
                VariableLengthQuantity.WriteVariableLengthInt((uint) bytes.Length, sliceOutput);
                sliceOutput.WriteBytes(bytes);
            });

        public static readonly VersionEditTag LogNumber = new VersionEditTag(2,
            (sliceInput, versionEdit) =>
            {
                versionEdit.LogNumber = (long) VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
            },
            (sliceOutput, versionEdit, self) =>
            {
                var logNumber = versionEdit.LogNumber;
                if (logNumber != null)
                {
                    VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                    VariableLengthQuantity.WriteVariableLengthLong((ulong) logNumber, sliceOutput);
                }
            });

        public static readonly VersionEditTag PreviousLogNumbe = new VersionEditTag(9,
            (sliceInput, versionEdit) =>
            {
                var previousLogNumber = VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
                versionEdit.PreviousLogNumber = (long) previousLogNumber;
            },
            (sliceOutput, versionEdit, self) =>
            {
                var previousLogNumber = versionEdit.PreviousLogNumber;
                if (previousLogNumber != null)
                {
                    VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                    VariableLengthQuantity.WriteVariableLengthLong((ulong) previousLogNumber, sliceOutput);
                }
            });

        public static readonly VersionEditTag NextFileNumber = new VersionEditTag(3,
            (sliceInput, versionEdit) =>
            {
                versionEdit.NextFileNumber = (long) VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
            },
            (sliceOutput, versionEdit, self) =>
            {
                var nextFileNumber = versionEdit.NextFileNumber;
                VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                VariableLengthQuantity.WriteVariableLengthLong((ulong) nextFileNumber, sliceOutput);
            });

        public static readonly VersionEditTag LastSequence = new VersionEditTag(4,
            (sliceInput, versionEdit) =>
            {
                versionEdit.LastSequenceNumber = (long) VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
            },
            (sliceOutput, versionEdit, self) =>
            {
                var lastSequenceNumber = versionEdit.LastSequenceNumber;
                VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                VariableLengthQuantity.WriteVariableLengthLong((ulong) lastSequenceNumber, sliceOutput);
            });

        public static readonly VersionEditTag CompactPointer = new VersionEditTag(5,
            (sliceInput, versionEdit) =>
            {
                // level
                var level = VariableLengthQuantity.ReadVariableLengthInt(sliceInput);
                // internal key
                var internalKey = new InternalKey(Slices.ReadLengthPrefixedBytes(sliceInput));
                versionEdit.SetCompactPointer((int) level, internalKey);
            },
            (sliceOutput, versionEdit, self) =>
            {
                foreach (var entry in versionEdit.GetCompactPointers())
                {
                    VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                    // level
                    VariableLengthQuantity.WriteVariableLengthInt((uint) entry.Key, sliceOutput);
                    // internal key
                    Slices.WriteLengthPrefixedBytes(sliceOutput, entry.Value.Encode());
                }
            });

        public static readonly VersionEditTag DeletedFile = new VersionEditTag(6,
            (sliceInput, versionEdit) =>
            {
                // level
                var level = VariableLengthQuantity.ReadVariableLengthInt(sliceInput);
                // internal key
                var fileNumber = VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
                versionEdit.DeleteFile((int) level, (long) fileNumber);
            },
            (sliceOutput, versionEdit, self) =>
            {
                foreach (var entry in versionEdit.DeletedFiles)
                {
                    foreach (var fileNumber in entry.Value)
                    {
                        VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                        // level
                        VariableLengthQuantity.WriteVariableLengthInt((uint) entry.Key, sliceOutput);
                        // file number
                        VariableLengthQuantity.WriteVariableLengthLong((ulong) fileNumber, sliceOutput);
                    }
                }
            });

        public static readonly VersionEditTag NewFile = new VersionEditTag(7,
            (sliceInput, versionEdit) =>
            {
                // level
                var level = (int) VariableLengthQuantity.ReadVariableLengthInt(sliceInput);
                // file number
                var fileNumber = (long) VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
                // file size
                var fileSize = (long) VariableLengthQuantity.ReadVariableLengthLong(sliceInput);
                // smallest key
                var smallestKey = new InternalKey(Slices.ReadLengthPrefixedBytes(sliceInput));
                // largest key
                var largestKey = new InternalKey(Slices.ReadLengthPrefixedBytes(sliceInput));

                versionEdit.AddFile(level, fileNumber, fileSize, smallestKey, largestKey);
            },
            (sliceOutput, versionEdit, self) =>
            {
                foreach (var entry in versionEdit.NewFiles)
                {
                    foreach (var fileMetaData in entry.Value)
                    {
                        VariableLengthQuantity.WriteVariableLengthInt((uint) self.PersistentId, sliceOutput);
                        // level
                        VariableLengthQuantity.WriteVariableLengthInt((uint) entry.Key, sliceOutput);
                        // file number
                        VariableLengthQuantity.WriteVariableLengthLong((ulong) fileMetaData.Number, sliceOutput);
                        // file size
                        VariableLengthQuantity.WriteVariableLengthLong((ulong) fileMetaData.FileSize, sliceOutput);
                        // smallest key
                        Slices.WriteLengthPrefixedBytes(sliceOutput, fileMetaData.Smallest.Encode());
                        // smallest key
                        Slices.WriteLengthPrefixedBytes(sliceOutput, fileMetaData.Largest.Encode());
                    }
                }
            });

        public static IEnumerable<VersionEditTag> Values()
        {
            yield return Comparator;
            yield return LogNumber;
            yield return PreviousLogNumbe;
            yield return NextFileNumber;
            yield return LastSequence;
            yield return CompactPointer;
            yield return DeletedFile;
            yield return NewFile;
        }

        public static VersionEditTag GetValueTypeByPersistentId(int persistentId)
        {
            foreach (var compressionType in Values())
            {
                if (compressionType.PersistentId == persistentId)
                {
                    return compressionType;
                }
            }
            throw new ArgumentException($"Unknown {typeof(VersionEditTag)} persistentId {persistentId}");
        }

        public int PersistentId { get; }
        private readonly Action<SliceInput, VersionEdit> _readValueAction;
        private readonly Action<SliceOutput, VersionEdit, VersionEditTag> _writeValueAction;

        private VersionEditTag(int persistentId, Action<SliceInput, VersionEdit> readValueAction,
            Action<SliceOutput, VersionEdit, VersionEditTag> writeValueAction)
        {
            _readValueAction = readValueAction;
            _writeValueAction = writeValueAction;
            PersistentId = persistentId;
        }

        public void ReadValue(SliceInput sliceInput, VersionEdit versionEdit)
        {
            _readValueAction(sliceInput, versionEdit);
        }

        public void WriteValue(SliceOutput sliceOutput, VersionEdit versionEdit)
        {
            _writeValueAction(sliceOutput, versionEdit, this);
        }
    }
}