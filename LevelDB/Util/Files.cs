using System.IO;
using System.Text;

namespace LevelDB.Util
{
    public static class Files
    {
        public static void Write(string text, FileInfo fileInfo, Encoding encoding)
        {
            using (var tempStream = fileInfo.OpenWrite())
            using (var streamWriter = new StreamWriter(tempStream, encoding))
            {
                streamWriter.Write(text);
                streamWriter.Flush();
            }
        }
    }
}