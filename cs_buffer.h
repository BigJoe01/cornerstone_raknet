/**
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  The ASF licenses
* this file to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#ifndef _BUFFER_HXX_
#define _BUFFER_HXX_

namespace cornerstone {
    class CBuffer {
        CBuffer() = delete;
        __nocopy__(CBuffer)
    public:
        static CPtr<CBuffer> alloc(const size_t size);
        static CPtr<CBuffer> copy(const CBuffer& buf);
        
        size_t size() const;
        size_t Pos() const;
        void Pos(size_t p);

        int32 GetInt();
        ulong GetULong();
        byte GetByte();
        void Get(CPtr<CBuffer>& dst);
        const char* GetStr();
        byte* Data() const;

        void Put(byte b);
        void Put(int32 val);
        void Put(ulong val);
        void Put(const std::string& str);
        void Put(const CBuffer& buf);
    };

    std::ostream& operator << (std::ostream& out, CBuffer& buf);
    std::istream& operator >> (std::istream& in, CBuffer& buf);
}
#endif //_BUFFER_HXX_