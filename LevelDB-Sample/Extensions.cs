using System.Text;

namespace LevelDB.Sample
{
    public static class Extensions
    {
        public static byte[] GetBytes(this string str)
        {
            return GetBytes(str, Encoding.UTF8);
        }
        
        public static byte[] GetBytes(this string str, Encoding encoding)
        {
            return encoding.GetBytes(str);
        }

    }
}