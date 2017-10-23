using System;
using System.IO;
using LevelDB.Impl;

namespace LevelDB.Sample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var databaseDir = new DirectoryInfo("test1");

            var options = new Options().CreateIfMissing(true);
            using (var db = new DBFactory().Open(databaseDir, options))
            {
                db.Put("testkey".GetBytes(), "testvalue".GetBytes());
            }

            using (var db = new DBFactory().Open(databaseDir, options))
            {
                Console.WriteLine(db.Get("testkey".GetBytes()).ToString());
            }
        }
    }
}