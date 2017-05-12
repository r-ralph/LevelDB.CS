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
using System.Threading;

namespace LevelDB.Impl
{
    public class FileMetaData
    {
        public static readonly Func<FileMetaData, InternalKey> GetLargestUserKey = fileMetaData => fileMetaData.Largest;

        public long Number { get; }

        /// <summary>
        /// File size in bytes
        /// </summary>
        public long FileSize { get; }

        /// <summary>
        /// Smallest internal key served by table
        /// </summary>
        public InternalKey Smallest { get; }

        /// <summary>
        /// Largest internal key served by table
        /// </summary>
        public InternalKey Largest { get; }

        /// <summary>
        /// Seeks allowed until compaction
        /// </summary>
        /// TODO: this mutable state should be moved elsewhere
        public int AllowedSeeks
        {
            get => _allowedSeeks;
            set => Interlocked.Exchange(ref _allowedSeeks, value);
        }

        private int _allowedSeeks = 1 << 30;

        public FileMetaData(long number, long fileSize, InternalKey smallest, InternalKey largest)
        {
            Number = number;
            FileSize = fileSize;
            Smallest = smallest;
            Largest = largest;
        }

        public void DecrementAllowedSeeks()
        {
            Interlocked.Decrement(ref _allowedSeeks);
        }

        public override string ToString()
        {
            return
                $"FileMetaData(number={Number}, fileSize={FileSize}, smallest={Smallest}, largest={Largest}, allowedSeeks={AllowedSeeks})";
        }
    }
}