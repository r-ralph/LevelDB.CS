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

namespace LevelDB.Util
{
    public interface IDataInput
    {
        void ReadFully(byte[] b);

        void ReadFully(byte[] b, int off, int len);

        int SkipBytes(int n);

        bool ReadBoolean();

        byte ReadByteAlt();

        sbyte ReadSignedByte();

        short ReadShort();

        ushort ReadUnsignedShort();

        char ReadChar();

        int ReadInt();

        uint ReadUnsignedInt();

        long ReadLong();

        ulong ReadUnsignedLong();

        float ReadFloat();

        double ReadDouble();

        string ReadLine();

        string ReadUTF();
    }
}