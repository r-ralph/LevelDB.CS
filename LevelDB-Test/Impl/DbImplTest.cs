using System;
using System.IO;
using System.Text;
using LevelDB.Util;
using Xunit;
using static LevelDB.InnerUtil.GenericUtil;

namespace LevelDB.Impl
{
    public class DbImplTest
    {
        private static readonly double StressFactor =
            double.Parse(Environment.GetEnvironmentVariable("STRESS_FACTOR") ?? "1");

        private readonly DirectoryInfo _databaseDir;

        public DbImplTest()
        {
            var tempDirectory = new DirectoryInfo(Path.Combine(Path.GetTempPath(), Path.GetRandomFileName()));
            tempDirectory.Create();
            _databaseDir = tempDirectory;
        }

        [Fact]
        public void TestBackgroundCompaction()
        {
            var options = new Options();
            options.MaxOpenFiles(100);
            options.CreateIfMissing(true);
            var db = new DbImpl(options, _databaseDir);
            var random = new Random(301);
            for (var i = 0; i < 2000 * StressFactor; i++)
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

        /**
         *  @Test
    public void testCompactionsOnBigDataSet()
            throws Exception
    {
        Options options = new Options();
        options.createIfMissing(true);
        DbImpl db = new DbImpl(options, databaseDir);
        for (int index = 0; index < 5000000; index++) {
            String key = "Key LOOOOOOOOOOOOOOOOOONG KEY " + index;
            String value = "This is element " + index + "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABZASDFASDKLFJASDFKJSDFLKSDJFLKJSDHFLKJHSDJFSDFHJASDFLKJSDF";
            db.Put(key.getBytes("UTF-8"), value.getBytes("UTF-8"));
        }
    }
         */

        [Fact]
        public void TestEmpty()
        {
            var options = new Options();
            var databaseDir = _databaseDir;
            var db = new DbStringWrapper(options, databaseDir);
            Assert.Null(db.Get("foo"));
        }

        [Fact]
        public void TestEmptyBatch()
        {
            // open new db
            var options = new Options().CreateIfMissing(true);
            var db = new DBFactory().Open(_databaseDir, options);

            // write an empty batch
            var batch = db.CreateWriteBatch();
            batch.Dispose();
            db.Write(batch);

            // close the db
            db.Dispose();

            // reopen db
            new DBFactory().Open(_databaseDir, options);
        }

        [Fact]
        public void TestReadWrite()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);
            db.Put("foo", "v1");
            Assert.Equal(db.Get("foo"), "v1");
            db.Put("bar", "v2");
            db.Put("foo", "v3");
            Assert.Equal(db.Get("foo"), "v3");
            Assert.Equal(db.Get("bar"), "v2");
        }

        [Fact]
        public void TestPutDeleteGet()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);
            db.Put("foo", "v1");
            Assert.Equal(db.Get("foo"), "v1");
            db.Put("foo", "v2");
            Assert.Equal(db.Get("foo"), "v2");
            db.Delete("foo");
            Assert.Null(db.Get("foo"));
        }

        [Fact]
        public void TestGetFromImmutableLayer()
        {
            // create db with small write buffer
            var db = new DbStringWrapper(new Options().WriteBufferSize(100000), _databaseDir);
            db.Put("foo", "v1");
            Assert.Equal(db.Get("foo"), "v1");

            // todo Block sync calls

            // Fill memtable
            db.Put("k1", LongString(100000, 'x'));
            // Trigger compaction
            db.Put("k2", LongString(100000, 'y'));
            Assert.Equal(db.Get("foo"), "v1");

            // todo Release sync calls
        }

        [Fact]
        public void TestGetFromVersions()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);
            db.Put("foo", "v1");
            db.CompactMemTable();
            Assert.Equal(db.Get("foo"), "v1");
        }

        [Fact]
        public void TestGetSnapshot()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);

            // Try with both a short key and a long key
            for (var i = 0; i < 2; i++)
            {
                var key = (i == 0) ? "foo" : LongString(200, 'x');
                db.Put(key, "v1");
                using (var s1 = db.GetSnapshot())
                {
                    db.Put(key, "v2");
                    Assert.Equal(db.Get(key), "v2");
                    Assert.Equal(db.Get(key, s1), "v1");

                    db.CompactMemTable();
                    Assert.Equal(db.Get(key), "v2");
                    Assert.Equal(db.Get(key, s1), "v1");
                }
            }
        }

        [Fact]
        public void TestGetLevel0Ordering()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);

            // Check that we process level-0 files in correct order.  The code
            // below generates two level-0 files where the earlier one comes
            // before the later one in the level-0 file list since the earlier
            // one has a smaller "smallest" key.
            db.Put("bar", "b");
            db.Put("foo", "v1");
            db.CompactMemTable();
            db.Put("foo", "v2");
            db.CompactMemTable();
            Assert.Equal(db.Get("foo"), "v2");
        }

        [Fact]
        public void TestGetOrderedByLevels()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);
            db.Put("foo", "v1");
            db.Compact("a", "z");
            Assert.Equal(db.Get("foo"), "v1");
            db.Put("foo", "v2");
            Assert.Equal(db.Get("foo"), "v2");
            db.CompactMemTable();
            Assert.Equal(db.Get("foo"), "v2");
        }

        [Fact]
        public void TestGetPicksCorrectFile()
        {
            var db = new DbStringWrapper(new Options(), _databaseDir);
            db.Put("a", "va");
            db.Compact("a", "b");
            db.Put("x", "vx");
            db.Compact("x", "y");
            db.Put("f", "vf");
            db.Compact("f", "g");

            Assert.Equal(db.Get("a"), "va");
            Assert.Equal(db.Get("f"), "vf");
            Assert.Equal(db.Get("x"), "vx");
        }


        private static string LongString(int length, char character)
        {
            var chars = new char[length];
            for (var i = 0; i < length; i++)
            {
                chars[i] = character;
            }
            return new string(chars);
        }
    }
}