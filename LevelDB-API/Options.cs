using System;

namespace LevelDB
{
    public class Options
    {
        private bool _createIfMissing = true;
        private bool _errorIfExists;
        private int _writeBufferSize = 4 << 20;
        private int _maxOpenFiles = 1000;
        private int _blockRestartInterval = 16;
        private int _blockSize = 4 * 1024;
        private CompressionType _compressionType = LevelDB.CompressionType.Snappy;
        private bool _verifyChecksums = true;
        private bool _paranoidChecks;
        private IDBComparator _comparator;
        private ILogger _logger;
        private long _cacheSize;

        public static void CheckArgNotNull(object value, string name)
        {
            if (value == null)
            {
                throw new ArgumentException("The " + name + " argument cannot be null");
            }
        }

        public bool CreateIfMissing()
        {
            return _createIfMissing;
        }

        public Options CreateIfMissing(bool createIfMissing)
        {
            _createIfMissing = createIfMissing;
            return this;
        }

        public bool ErrorIfExists()
        {
            return _errorIfExists;
        }

        public Options ErrorIfExists(bool errorIfExists)
        {
            _errorIfExists = errorIfExists;
            return this;
        }

        public int WriteBufferSize()
        {
            return _writeBufferSize;
        }

        public Options WriteBufferSize(int writeBufferSize)
        {
            _writeBufferSize = writeBufferSize;
            return this;
        }

        public int MaxOpenFiles()
        {
            return _maxOpenFiles;
        }

        public Options MaxOpenFiles(int maxOpenFiles)
        {
            _maxOpenFiles = maxOpenFiles;
            return this;
        }

        public int BlockRestartInterval()
        {
            return _blockRestartInterval;
        }

        public Options BlockRestartInterval(int blockRestartInterval)
        {
            _blockRestartInterval = blockRestartInterval;
            return this;
        }

        public int BlockSize()
        {
            return _blockSize;
        }

        public Options BlockSize(int blockSize)
        {
            _blockSize = blockSize;
            return this;
        }

        public CompressionType CompressionType()
        {
            return _compressionType;
        }

        public Options CompressionType(CompressionType compressionType)
        {
            CheckArgNotNull(compressionType, "compressionType");
            _compressionType = compressionType;
            return this;
        }

        public bool VerifyChecksums()
        {
            return _verifyChecksums;
        }

        public Options VerifyChecksums(bool verifyChecksums)
        {
            _verifyChecksums = verifyChecksums;
            return this;
        }

        public long CacheSize()
        {
            return _cacheSize;
        }

        public Options CacheSize(long cacheSize)
        {
            _cacheSize = cacheSize;
            return this;
        }

        public IDBComparator Comparator()
        {
            return _comparator;
        }

        public Options Comparator(IDBComparator comparator)
        {
            _comparator = comparator;
            return this;
        }

        public ILogger Logger()
        {
            return _logger;
        }

        public Options Logger(ILogger logger)
        {
            _logger = logger;
            return this;
        }

        public bool ParanoidChecks()
        {
            return _paranoidChecks;
        }

        public Options ParanoidChecks(bool paranoidChecks)
        {
            _paranoidChecks = paranoidChecks;
            return this;
        }
    }
}