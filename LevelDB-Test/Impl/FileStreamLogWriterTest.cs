using System.IO;
using LevelDB.Util;
using Xunit;

namespace LevelDB.Impl
{
    public class FileStreamLogWriterTest
    {
        [Fact]
        public void TestLogRecordBounds()
        {
            var file = new FileInfo(Path.GetTempFileName());
            try
            {
                var recordSize = LogConstants.BlockSize - LogConstants.HeaderSize;
                var record = new Slice(recordSize);

                ILogWriter writer = new FileStreamLogWriter(file, 10);
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

        private class AssertNoCorruptionLogMonitor : LogMonitor
        {
            public AssertNoCorruptionLogMonitor() : base(
                (bytes, reason) => { Assert.True(false, "corruption at " + bytes + " reason: " + reason); },
                (bytes, reason) => { Assert.True(false, "corruption at " + bytes + " reason: " + reason); })
            {
            }
        }
    }
}