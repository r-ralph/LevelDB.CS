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

namespace LevelDB.Impl
{
    public sealed class LogChunkType
    {
        public static readonly LogChunkType ZeroType = new LogChunkType(0);
        public static readonly LogChunkType Full = new LogChunkType(1);
        public static readonly LogChunkType First = new LogChunkType(2);
        public static readonly LogChunkType Middle = new LogChunkType(3);
        public static readonly LogChunkType Last = new LogChunkType(4);
        public static readonly LogChunkType Eof = new LogChunkType();
        public static readonly LogChunkType BadChunk = new LogChunkType();
        public static readonly LogChunkType Unknown = new LogChunkType();

        public static LogChunkType GetLogChunkTypeByPersistentId(int persistentId)
        {
            // ReSharper disable once SwitchStatementMissingSomeCases
            switch (persistentId)
            {
                case 0:
                    return ZeroType;
                case 1:
                    return Full;
                case 2:
                    return First;
                case 3:
                    return Middle;
                case 4:
                    return Last;
            }
            return Unknown;
        }

        private readonly int? _persistentId;

        public int PersistentId
        {
            get
            {
                Preconditions.CheckArgument(_persistentId != null, "This type is not a persistent chunk type");
                // ReSharper disable once PossibleInvalidOperationException
                return _persistentId.Value;
            }
        }

        private LogChunkType()
        {
            _persistentId = null;
        }

        private LogChunkType(int persistentId)
        {
            _persistentId = persistentId;
        }
    }
}