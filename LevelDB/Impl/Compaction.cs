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
using System.Linq;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    /// <summary>
    /// A Compaction encapsulates information about a compaction.
    /// </summary>
    public class Compaction
    {
        public int Level { get; }

        /// <summary>
        /// Each compaction reads inputs from "level" and "level+1"
        /// </summary>
        public IList<FileMetaData> LevelInputs { get; }

        public IList<FileMetaData> LevelUpInputs { get; }

        public IList<FileMetaData>[] Inputs { get; }

        public VersionEdit Edit { get; } = new VersionEdit();

        /// <summary>
        /// Maximum size of files to build during this compaction.
        /// </summary>
        public long MaxOutputFileSize { get; }

        private readonly Version _inputVersion;
        private readonly IList<FileMetaData> _grandparents;

        // State used to check for number of of overlapping grandparent files
        // (parent == level_ + 1, grandparent == level_ + 2)

        /// <summary>
        /// Index in grandparent_starts_
        /// </summary>
        private int _grandparentIndex;

        /// <summary>
        /// Some output key has been seen
        /// </summary>
        private bool _seenKey;

        /// <summary>
        /// Bytes of overlap between current output and grandparent files
        /// </summary>
        private long _overlappedBytes;

        // State for implementing IsBaseLevelForKey

        /// <summary>
        /// levelPointers holds indices into inputVersion -> levels: our state
        /// is that we are positioned at one of the file ranges for each
        /// higher level than the ones involved in this compaction (i.e. for
        /// all L >= level_ + 2).
        /// </summary>
        private readonly int[] _levelPointers = new int[DbConstants.NumLevels];

        public Compaction(Version inputVersion, int level, IList<FileMetaData> levelInputs,
            IList<FileMetaData> levelUpInputs, IList<FileMetaData> grandparents)
        {
            _inputVersion = inputVersion;
            Level = level;
            LevelInputs = levelInputs;
            LevelUpInputs = levelUpInputs;
            _grandparents = grandparents;
            MaxOutputFileSize = VersionSet.MaxFileSizeForLevel(level);
            Inputs = new[] {levelInputs, levelUpInputs};
        }

        /// <summary>
        /// Return the ith input file at "level()+which" ("which" must be 0 or 1).
        /// </summary>
        /// <param name="which"></param>
        /// <param name="i"></param>
        /// <returns></returns>
        public FileMetaData Input(int which, int i)
        {
            Preconditions.CheckArgument(which == 0 || which == 1, "which must be either 0 or 1");
            return which == 0 ? LevelInputs[i] : LevelUpInputs[i];
        }

        public bool IsTrivialMove()
        {
            // Avoid a move if there is lots of overlapping grandparent data.
            // Otherwise, the move could create a parent file that will require
            // a very expensive merge later on.
            return LevelInputs.Count == 1 &&
                   LevelUpInputs.Count == 0 &&
                   TotalFileSize(_grandparents) <= VersionSet.MaxGrandParentOverlapBytes;
        }

        public static long TotalFileSize(IList<FileMetaData> files)
        {
            return files.Sum(file => file.FileSize);
        }

        /// <summary>
        /// Add all inputs to this compaction as delete operations to *edit.
        /// </summary>
        /// <param name="edit"></param>
        public void AddInputDeletions(VersionEdit edit)
        {
            foreach (var input in LevelInputs)
            {
                edit.DeleteFile(Level, input.Number);
            }
            foreach (var input in LevelUpInputs)
            {
                edit.DeleteFile(Level + 1, input.Number);
            }
        }

        /// <summary>
        /// Returns true if the information we have available guarantees that
        /// the compaction is producing data in "level+1" for which no data exists
        /// in levels greater than "level+1".
        /// </summary>
        /// <param name="userKey"></param>
        /// <returns></returns>
        public bool IsBaseLevelForKey(Slice userKey)
        {
            // Maybe use binary search to find right entry instead of linear search?
            var userComparator = _inputVersion.InternalKeyComparator.UserComparator;
            for (var level = Level + 2; level < DbConstants.NumLevels; level++)
            {
                List<FileMetaData> files = _inputVersion.GetFiles(level);
                while (_levelPointers[level] < files.Count)
                {
                    var f = files[_levelPointers[level]];
                    if (userComparator.Compare(userKey, f.Largest.UserKey) <= 0)
                    {
                        // We've advanced far enough
                        if (userComparator.Compare(userKey, f.Smallest.UserKey) >= 0)
                        {
                            // Key falls in this file's range, so definitely not base level
                            return false;
                        }
                        break;
                    }
                    _levelPointers[level]++;
                }
            }
            return true;
        }

        /// <summary>
        /// Returns true iff we should stop building the current output
        /// before processing "internal_key".
        /// </summary>
        /// <param name="internalKey"></param>
        /// <returns></returns>
        public bool ShouldStopBefore(InternalKey internalKey)
        {
            if (_grandparents == null)
            {
                return false;
            }

            // Scan to find earliest grandparent file that contains key.
            var internalKeyComparator = _inputVersion.InternalKeyComparator;
            while (_grandparentIndex < _grandparents.Count &&
                   internalKeyComparator.Compare(internalKey, _grandparents[_grandparentIndex].Largest) > 0)
            {
                if (_seenKey)
                {
                    _overlappedBytes += _grandparents[_grandparentIndex].FileSize;
                }
                _grandparentIndex++;
            }
            _seenKey = true;

            if (_overlappedBytes <= VersionSet.MaxGrandParentOverlapBytes) return false;
            // Too much overlap for current output; start new output
            _overlappedBytes = 0;
            return true;
        }
    }
}