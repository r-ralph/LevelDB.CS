using System;

namespace LevelDB
{
    public class DBException : Exception
    {
        public DBException()
        {
        }

        public DBException(string s) : base(s)
        {
        }

        public DBException(string s, Exception e) : base(s, e)
        {
        }
    }
}