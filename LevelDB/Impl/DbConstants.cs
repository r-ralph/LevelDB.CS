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

namespace LevelDB.Impl
{
    public static class DbConstants
    {
        public const int MajorVersion = 0;
        public const int MinorVersion = 1;

        // TODO: this should be part of the configuration

        /// <summary>
        /// Max number of levels
        /// </summary>
        public const int NumLevels = 7;

        /// <summary>
        /// Level-0 compaction is started when we hit this many files.
        /// </summary>
        public const int L0CompactionTrigger = 4;

        /// <summary>
        /// Soft limit on number of level-0 files.  We slow down writes at this point.
        /// </summary>
        public const int L0SlowdownWritesTrigger = 8;

        /// <summary>
        /// Maximum number of level-0 files.  We stop writes at this point.
        /// </summary>
        public const int L0StopWritesTrigger = 12;

        /// <summary>
        /// Maximum level to which a new compacted memtable is pushed if it
        /// does not create overlap.  We try to push to level 2 to avoid the
        /// relatively expensive level 0=>1 compactions and to avoid some
        /// expensive manifest file operations.  We do not push all the way to
        /// the largest level since that can generate a lot of wasted disk
        /// space if the same key space is being repeatedly overwritten.
        /// </summary>
        public const int MaxMemCompactLevel = 2;
    }
}