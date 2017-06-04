using System;

namespace LevelDB.InnerUtil
{
    public static class GenericUtil
    {
        public static string RandomString(Random random, int length)
        {
            char[] chars = new char[length];
            for (int i = 0; i < chars.Length; i++)
            {
                chars[i] = (char) (' ' + random.Next(95));
            }
            return new String(chars);
        }
    }
}