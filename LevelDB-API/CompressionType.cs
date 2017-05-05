using System;
using System.Collections.Generic;

namespace LevelDB
{
    public class CompressionType
    {
        public static readonly CompressionType None = new CompressionType(0x00);
        public static readonly CompressionType Snappy = new CompressionType(0x01);
        public static readonly CompressionType Zlib = new CompressionType(0x02);

        public static IEnumerable<CompressionType> Values
        {
            get
            {
                yield return None;
                yield return Snappy;
                yield return Zlib;
            }
        }

        public int PersistentId { get; }

        public static CompressionType GetCompressionTypeByPersistentId(int persistentId)
        {
            foreach (var compressionType in Values)
            {
                if (compressionType.PersistentId == persistentId)
                {
                    return compressionType;
                }
            }
            throw new ArgumentException("Unknown persistent id :" + persistentId);
        }

        public CompressionType(int persistentId)
        {
            PersistentId = persistentId;
        }
    }
}