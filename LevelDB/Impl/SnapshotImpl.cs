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

using LevelDB.Util.Atomic;

namespace LevelDB.Impl
{
    public class SnapshotImpl : ISnapshot
    {
        private readonly AtomicBoolean _closed = new AtomicBoolean();
        private readonly Version _version;
        private readonly long _lastSequence;

        public SnapshotImpl(Version version, long lastSequence)
        {
            _version = version;
            _lastSequence = lastSequence;
            _version.Retain();
        }

        public void Close()
        {
            // This is an end user API.. he might screw up and close multiple times.
            // but we don't want the version reference count going bad.
            if (_closed.CompareAndSet(false, true))
            {
                _version.Release();
            }
        }

        public long GetLastSequence()
        {
            return _lastSequence;
        }

        public Version GetVersion()
        {
            return _version;
        }

        public override string ToString()
        {
            return _lastSequence.ToString();
        }

        public void Dispose()
        {
        }

        public override bool Equals(object o)
        {
            if (this == o)
            {
                return true;
            }

            var snapshot = o as SnapshotImpl;

            return _lastSequence == snapshot?._lastSequence && _version.Equals(snapshot._version);
        }

        public override int GetHashCode()
        {
            var result = _version.GetHashCode();
            result = 31 * result + (int) (_lastSequence ^ (_lastSequence >> 32));
            return result;
        }
    }
}