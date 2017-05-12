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

using System.Diagnostics.CodeAnalysis;
using System.Text;
using LevelDB.Guava;
using LevelDB.Util;

namespace LevelDB.Impl
{
    public class InternalKey
    {
        public Slice UserKey { get; }

        public long SequenceNumber { get; }

        public ValueType ValueType { get; }

        private int _hash;

        public InternalKey(Slice userKey, long sequenceNumber, ValueType valueType)
        {
            Preconditions.CheckNotNull(userKey, $"{userKey} is null");
            Preconditions.CheckArgument(sequenceNumber >= 0, $"{sequenceNumber} is negative");
            Preconditions.CheckNotNull(valueType, $"{valueType} is null");

            UserKey = userKey;
            SequenceNumber = sequenceNumber;
            ValueType = valueType;
        }

        public InternalKey(Slice data)
        {
            Preconditions.CheckNotNull(data, "data is null");
            Preconditions.CheckArgument(data.Length >= SizeOf.Long, "data must be at least %s bytes", SizeOf.Long);
            UserKey = GetUserKey(data);
            var packedSequenceAndType = data.GetLong(data.Length - SizeOf.Long);
            SequenceNumber = Impl.SequenceNumber.UnpackSequenceNumber(packedSequenceAndType);
            ValueType = Impl.SequenceNumber.UnpackValueType(packedSequenceAndType);
        }

        public InternalKey(byte[] data) : this(Slices.WrappedBuffer(data))
        {
        }

        public Slice Encode()
        {
            var slice = Slices.Allocate(UserKey.Length + SizeOf.Long);
            var sliceOutput = slice.Output();
            sliceOutput.WriteBytes(UserKey);
            sliceOutput.WriteLong(Impl.SequenceNumber.PackSequenceAndValueType(SequenceNumber, ValueType));
            return slice;
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }
            var that = o as InternalKey;
            if (SequenceNumber != that?.SequenceNumber)
            {
                return false;
            }
            if (!UserKey?.Equals(that.UserKey) ?? that.UserKey != null)
            {
                return false;
            }
            return ValueType == that.ValueType;
        }

        [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
        public override int GetHashCode()
        {
            if (_hash != 0) return _hash;
            var result = UserKey != null ? UserKey.GetHashCode() : 0;
            result = 31 * result + (int) (SequenceNumber ^ (long) ((ulong) SequenceNumber >> 32));
            result = 31 * result + (ValueType != null ? ValueType.GetHashCode() : 0);
            if (result == 0)
            {
                result = 1;
            }
            _hash = result;
            return _hash;
        }

        public override string ToString()
        {
            return
                $"InternalKey(key={UserKey.ToString(Encoding.UTF8)}, sequenceNumber={SequenceNumber}, valueType={ValueType})";
        }

        private static Slice GetUserKey(Slice data)
        {
            return data.Sliced(0, data.Length - SizeOf.Long);
        }
    }
}