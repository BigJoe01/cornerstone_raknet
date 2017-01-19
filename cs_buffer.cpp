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

#include "cornerstone/cornerstone.h"

#define __is_big_block(p) (0x80000000 & *((uint*)(p)))
#define __init_block(p, s, t) ((t*)(p))[0] = (t)s;\
    ((t*)(p))[1] = 0
#define __init_s_block(p, s) __init_block(p, s, ushort)
#define __init_b_block(p, s) __init_block(p, s, uint);\
    *((uint*)(p)) |= 0x80000000
#define __pos_of_s_block(p) ((ushort*)(p))[1]
#define __pos_of_b_block(p) ((uint*)(p))[1]
#define __size_of_block(p) (__is_big_block(p)) ? (*((uint*)(p)) ^ 0x80000000) : *((ushort*)(p))
#define __pos_of_block(p) (__is_big_block(p)) ? __pos_of_b_block(p) : __pos_of_s_block(p)
#define __mv_fw_block(p, d) if(__is_big_block(p)){\
    ((uint*)(p))[1] += (d);\
    }\
    else{\
    ((ushort*)(p))[1] += (ushort)(d);\
    }
#define __set_block_pos(p, pos) if(__is_big_block(p)){\
    ((uint*)(p))[1] = (pos);\
    }\
    else{\
    ((ushort*)(p))[1] = (ushort)(pos);\
    }
#define __data_of_block(p) (__is_big_block(p)) ? (byte*) (((byte*)(((uint*)(p)) + 2)) + __pos_of_b_block(p)) : (byte*) (((byte*)(((ushort*)p) + 2)) + __pos_of_s_block(p))
using namespace cornerstone;

CPtr<CBuffer> CBuffer::alloc(const size_t size) {
    if (size >= 0x80000000) {
        throw std::out_of_range("size exceed the max size that cornrestone::buffer could support");
    }

    if (size >= 0x8000) {
        CPtr<CBuffer> buf = cs_alloc<CBuffer>(size + sizeof(uint) * 2);
        any_ptr ptr = reinterpret_cast<any_ptr>(buf.get());
        __init_b_block(ptr, size);
        return buf;
    }

    CPtr<CBuffer> buf = cs_alloc<CBuffer>(size + sizeof(ushort) * 2);
    any_ptr ptr = reinterpret_cast<any_ptr>(buf.get());
    __init_s_block(ptr, size);
    return buf;
}

CPtr<CBuffer> CBuffer::copy(const CBuffer& buf) {
    CPtr<CBuffer> other = alloc(buf.size() - buf.Pos());
    other->Put(buf);
    other->Pos(0);
    return other;
}

size_t CBuffer::size() const {
    return (size_t)(__size_of_block(this));
}

size_t CBuffer::Pos() const {
    return (size_t)(__pos_of_block(this));
}

byte* CBuffer::Data() const {
    return __data_of_block(this);
}

int32 CBuffer::GetInt() {
    size_t avail = size() - Pos();
    if (avail < sz_int) {
        throw std::overflow_error("insufficient buffer available for an int32 value");
    }

    byte* d = Data();
    int32 val = 0;
    for (size_t i = 0; i < sz_int; ++i) {
        int32 byte_val = (int32)*(d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_int);
    return val;
}

ulong CBuffer::GetULong() {
    size_t avail = size() - Pos();
    if (avail < sz_ulong) {
        throw std::overflow_error("insufficient buffer available for an ulong value");
    }

    byte* d = Data();
    ulong val = 0L;
    for (size_t i = 0; i < sz_ulong; ++i) {
        ulong byte_val = (ulong)*(d + i);
        val += (byte_val << (i * 8));
    }

    __mv_fw_block(this, sz_ulong);
    return val;
}

byte CBuffer::GetByte() {
    size_t avail = size() - Pos();
    if (avail < sz_byte) {
        throw std::overflow_error("insufficient buffer available for a byte");
    }

    byte val = *Data();
    __mv_fw_block(this, sz_byte);
    return val;
}

void CBuffer::Pos(size_t p) {
    size_t position = p > size() ? size() : p;
    __set_block_pos(this, position);
}

const char* CBuffer::GetStr() {
    size_t p = Pos();
    size_t s = size();
    size_t i = 0;
    byte* d = Data();
    while ((p + i) < s && *(d + i)) ++i;
    if (p + i >= s || i == 0) {
        return nilptr;
    }

    __mv_fw_block(this, i + 1);
    return reinterpret_cast<const char*>(d);
}

void CBuffer::Put(byte b) {
    if (size() - Pos() < sz_byte) {
        throw std::overflow_error("insufficient buffer to store byte");
    }

    byte* d = Data();
    *d = b;
    __mv_fw_block(this, sz_byte);
}

void CBuffer::Put(int32 val) {
    if (size() - Pos() < sz_int) {
        throw std::overflow_error("insufficient buffer to store int32");
    }

    byte* d = Data();
    for (size_t i = 0; i < sz_int; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_int);
}

void CBuffer::Put(ulong val) {
    if (size() - Pos() < sz_ulong) {
        throw std::overflow_error("insufficient buffer to store int32");
    }

    byte* d = Data();
    for (size_t i = 0; i < sz_ulong; ++i) {
        *(d + i) = (byte)(val >> (i * 8));
    }

    __mv_fw_block(this, sz_ulong);
}

void CBuffer::Put(const std::string& str) {
    if (size() - Pos() < (str.length() + 1)) {
        throw std::overflow_error("insufficient buffer to store a string");
    }

    byte* d = Data();
    for (size_t i = 0; i < str.length(); ++i) {
        *(d + i) = (byte)str[i];
    }

    *(d + str.length()) = (byte)0;
    __mv_fw_block(this, str.length() + 1);
}

void CBuffer::Get(CPtr<CBuffer>& dst) {
    size_t sz = dst->size() - dst->Pos();
    ::memcpy(dst->Data(), Data(), sz);
    __mv_fw_block(this, sz);
}

void CBuffer::Put(const CBuffer& buf) {
    size_t sz = size();
    size_t p = Pos();
    size_t src_sz = buf.size();
    size_t src_p = buf.Pos();
    if ((sz - p) < (src_sz - src_p)) {
        throw std::overflow_error("insufficient buffer to hold the other buffer");
    }

    byte* d = Data();
    byte* src = buf.Data();
    ::memcpy(d, src, src_sz - src_p);
    __mv_fw_block(this, src_sz - src_p);
}

std::ostream& cornerstone::operator << (std::ostream& out, CBuffer& buf) {
    if (!out) {
        throw std::ios::failure("bad output stream.");
    }

    out.write(reinterpret_cast<char*>(buf.Data()), buf.size() - buf.Pos());

    if (!out) {
        throw std::ios::failure("write failed");
    }

    return out;
}

std::istream& cornerstone::operator >> (std::istream& in, CBuffer& buf) {
    if (!in) {
        throw std::ios::failure("bad input stream");
    }

    char* data = reinterpret_cast<char*>(buf.Data());
    int size = buf.size() - buf.Pos();
    in.read(data, size);

    if (!in) {
        throw std::ios::failure("read failed");
    }

    return in;
}