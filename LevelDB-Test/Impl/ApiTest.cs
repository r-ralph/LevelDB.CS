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
using System.IO;
using System.Text;
using LevelDB.Util;
using Xunit;

namespace LevelDB.Impl
{
    public class ApiTest: IDisposable
    {
        private readonly DBFactory _factory = DBFactory.Factory;
        private DirectoryInfo databaseDir;

        public ApiTest()
        {
            databaseDir = new DirectoryInfo(Path.Combine(Path.GetTempPath(), "leveldb"));
            FileUtil.DeleteRecursively(databaseDir);
            databaseDir.Create();
        }

        public void Dispose()
        {
            FileUtil.DeleteRecursively(databaseDir);
        }

        public static byte[] Bytes(string value)
        {
            return value == null ? null : Encoding.UTF8.GetBytes(value);
        }

        private DirectoryInfo GetTestDirectory(string name)
        {
            DirectoryInfo rc = new DirectoryInfo(Path.Combine(databaseDir.FullName, name));
            _factory.Destroy(rc, new Options().CreateIfMissing(true));
            rc.Create();
            return rc;
        }

    [Fact]
        public void TestCompaction()
        {
            Options options = new Options().CreateIfMissing(true).CompressionType(CompressionType.None);

            DirectoryInfo path = GetTestDirectory("testCompaction");
            using (var db = _factory.Open(path, options))
            {
                Console.WriteLine("Adding");
                for (int i = 0; i < 1000 * 1000; i++)
                {
                    if (i % 100000 == 0)
                    {
                        Console.WriteLine("  at: " + i);
                    }
                    db.Put(Bytes("key" + i), Bytes("value" + i));
                }
            }
            using (var db = _factory.Open(path, options))
            {

                Console.WriteLine("Deleting");
                for (int i = 0; i < 1000 * 1000; i++)
                {
                    if (i % 100000 == 0)
                    {
                        Console.WriteLine("  at: " + i);
                    }
                    db.Delete(Bytes("key" + i));
                }

            }
            using (var db = _factory.Open(path, options))
            {

                Console.WriteLine("Adding");
                for (int i = 0; i < 1000 * 1000; i++)
                {
                    if (i % 100000 == 0)
                    {
                        Console.WriteLine("  at: " + i);
                    }
                    db.Put(Bytes("key" + i), Bytes("value" + i));
                }

            }
        }
    }
}