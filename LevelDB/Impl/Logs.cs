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

using System.IO;
using LevelDB.Util;
using Ralph.Crc32C;

namespace LevelDB.Impl
{
    public static class Logs
    {
        public static ILogWriter CreateLogWriter(FileInfo file, long fileNumber)
        {
            if (DBFactory.UseMMap)
            {
                return new MMapLogWriter(file, fileNumber);
            }
            return new FileStreamLogWriter(file, fileNumber);
        }

        public static uint GetChunkChecksum(int chunkTypeId, Slice slice)
        {
            return GetChunkChecksum(chunkTypeId, slice.GetRawArray(), slice.GetRawOffset(), slice.Length);
        }

        public static uint GetChunkChecksum(int chunkTypeId, byte[] buffer, int offset, int length)
        {
            // Compute the crc of the record type and the payload.
            var crc32C = new Crc32C();
            crc32C.Update(chunkTypeId);
            crc32C.Update(buffer, offset, length);
            return crc32C.GetMaskedValue();
        }
    }
}