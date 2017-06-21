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
using LevelDB.InnerUtil;
using LevelDB.Util;
using Xunit;

namespace LevelDB.Impl
{
    public class MMapLogWriterTest
    {
        [Fact]
        public void TestLogRecordBounds()
        {
            var file = new FileInfo(Path.GetTempFileName());
            try
            {
                const int recordSize = LogConstants.BlockSize - LogConstants.HeaderSize;
                var record = new Slice(recordSize);

                ILogWriter writer = new MMapLogWriter(file, 10);
                writer.AddRecord(record, false);
                writer.Close();

                LogMonitor logMonitor = new AssertNoCorruptionLogMonitor();

                using (var channel = file.OpenRead())
                {
                    var logReader = new LogReader(channel, logMonitor, true, 0);

                    var count = 0;
                    for (var slice = logReader.ReadRecord(); slice != null;)
                    {
                        Assert.Equal(slice.Length, recordSize);
                        count++;
                        slice = logReader.ReadRecord();
                    }
                    Assert.Equal(count, 1);
                }
            }
            finally
            {
                file.Delete();
            }
        }
    }
}