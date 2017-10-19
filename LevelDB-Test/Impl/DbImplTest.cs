using System;
using System.IO;
using System.Text;
using Xunit;
using static LevelDB.InnerUtil.GenericUtil;

namespace LevelDB.Impl
{
    public class DbImplTest
    {
        public static readonly double StressFactor =
            Double.Parse(Environment.GetEnvironmentVariable("STRESS_FACTOR") ?? "1");

        private DirectoryInfo databaseDir;

        public DbImplTest()
        {
            var tempDirectory = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            tempDirectory.Create();
            databaseDir = tempDirectory;
        }

        [Fact]
        public void TestBackgroundCompaction()
        {
            Options options = new Options();
            options.MaxOpenFiles(100);
            options.CreateIfMissing(true);
            DbImpl db = new DbImpl(options, databaseDir);
            Random random = new Random(301);
            for (int i = 0; i < 2000 * StressFactor; i++)
            {
                db.Put(Encoding.UTF8.GetBytes(RandomString(random, 64)), new byte[] {0x01},
                    new WriteOptions().Sync(false));
                db.Get(Encoding.UTF8.GetBytes(RandomString(random, 64)));
                if (i % 1000 == 0 && i != 0)
                {
                    Console.WriteLine(i + " rows written");
                }
            }
        }
    }
}