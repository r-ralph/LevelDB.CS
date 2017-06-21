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
using System.IO;
using System.Linq;
using System.Text;
using LevelDB.InnerUtil;
using LevelDB.Util;
using Xunit;

namespace LevelDB.Impl
{
    public class LogTest : IDisposable
    {
        private ILogWriter writer;
        private AssertNoCorruptionLogMonitor _assertNoCorruptionLogMonitor;

        public LogTest()
        {
            writer = Logs.CreateLogWriter(new FileInfo(Path.GetTempFileName()), 42);
            _assertNoCorruptionLogMonitor = new AssertNoCorruptionLogMonitor();
        }

        public void Dispose()
        {
            writer?.Delete();
            _assertNoCorruptionLogMonitor = null;
        }

        [Fact]
        public void TestEmptyBlock()
        {
            TestLog();
        }

        [Fact]
        public void TestSmallRecord()
        {
            TestLog(ToSlice("dain sundstrom"));
        }

        [Fact]
        public void TestMultipleSmallRecords()
        {
            var records = new List<Slice>
            {
                ToSlice("Lagunitas  Little Sumpin' Sumpin'"),
                ToSlice("Lagunitas IPA"),
                ToSlice("Lagunitas Imperial Stout"),
                ToSlice("Oban 14"),
                ToSlice("Highland Park"),
                ToSlice("Lagavulin")
            };
            TestLog(records);
        }

        [Fact]
        public void TestLargeRecord()
        {
            TestLog(ToSlice("dain sundstrom", 4000));
        }

        [Fact]
        public void TestMultipleLargeRecords()
        {
            var records = new List<Slice>
            {
                ToSlice("Lagunitas  Little Sumpin’ Sumpin’", 4000),
                ToSlice("Lagunitas IPA", 4000),
                ToSlice("Lagunitas Imperial Stout", 4000),
                ToSlice("Oban 14", 4000),
                ToSlice("Highland Park", 4000),
                ToSlice("Lagavulin", 4000)
            };
            TestLog(records);
        }

        [Fact]
        public void TestReadWithoutProperClose()
        {
            TestLog(new List<Slice>
            {
                ToSlice("something"),
                ToSlice("something else")
            }, false);
        }

        private void TestLog(params Slice[] entries)
        {
            TestLog(entries.ToList());
        }

        private void TestLog(IList<Slice> records, bool closeWriter = true)
        {
            foreach (var entry in records)
            {
                writer.AddRecord(entry, false);
            }

            if (closeWriter)
            {
                writer.Close();
            }

            // test readRecord
            using (var fileChannel = writer.File.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                var reader = new LogReader(fileChannel, _assertNoCorruptionLogMonitor, true, 0);
                foreach (var expected in records)
                {
                    var actual = reader.ReadRecord();
                    Assert.Equal(expected, actual);
                }
                Assert.Null(reader.ReadRecord());
            }
        }

        private static Slice ToSlice(string value, int times = 1)
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            var slice = Slices.Allocate(bytes.Length * times);
            var sliceOutput = slice.Output();
            for (var i = 0; i < times; i++)
            {
                sliceOutput.WriteBytes(bytes);
            }
            return slice;
        }
    }
}