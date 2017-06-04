using System.Threading;

namespace LevelDB.Util.Atomic
{
    public class AtomicBoolean
    {
        private const int TrueValue = 1;
        private const int FalseValue = 0;
        private int _value = FalseValue;

        public AtomicBoolean()
            : this(false)
        {
        }

        public AtomicBoolean(bool initialValue)
        {
            Value = initialValue;
        }

        public bool Value
        {
            get => _value == TrueValue;
            set => _value = value ? TrueValue : FalseValue;
        }

        public bool CompareAndSet(bool expect, bool update)
        {
            var comparand = expect ? TrueValue : FalseValue;
            var result = Interlocked.CompareExchange(ref _value, update ? TrueValue : FalseValue, comparand);
            var originalValue = result == TrueValue;
            return originalValue == expect;
        }

        public bool GetAndSet(bool value)
        {
            return Interlocked.Exchange(ref _value, value ? TrueValue : FalseValue) == TrueValue;
        }
    }
}