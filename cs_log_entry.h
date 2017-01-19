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

#ifndef _LOG_ENTRY_HXX_
#define _LOG_ENTRY_HXX_
namespace cornerstone{
    class CLogEntry{
    public:
        CLogEntry(ulong term, const CPtr<CBuffer>& buff, ELogValueType value_type = ELogValueType::e_Application)
            : m_Term(term), m_eValueType(value_type), m_pBuff(buff) {
        }

    __nocopy__(CLogEntry)

    public:
        ulong GetTerm() const {
            return m_Term;
        }

        ELogValueType GetType() const {
            return m_eValueType;
        }

        CBuffer& GetBuffer() const {
            // we accept nil buffer, but in that case, the get_buf() shouldn't be called, throw runtime exception instead of having segment fault (AV on Windows)
            if (!m_pBuff) {
                throw std::runtime_error("get_buf cannot be called for a log_entry with nil buffer");
            }

            return *m_pBuff;
        }

        CPtr<CBuffer> Serialize() {
            m_pBuff->Pos(0);
            CPtr<CBuffer> buf = CBuffer::alloc(sizeof(ulong) + sizeof(char) + m_pBuff->size());
            buf->Put(m_Term);
            buf->Put((static_cast<byte>(m_eValueType)));
            buf->Put(*m_pBuff);
            buf->Pos(0);
            return buf;
        }

        static CPtr<CLogEntry> Deserialize(CBuffer& buf) {
            ulong term = buf.GetULong();
            ELogValueType t = static_cast<ELogValueType>(buf.GetByte());
            CPtr<CBuffer> data = CBuffer::copy(buf);
            return cs_new<CLogEntry>(term, data, t);
        }

        static ulong TermInBuffer(CBuffer& buf) {
            ulong term = buf.GetULong();
            buf.Pos(0); // reset the position
            return term;
        }

    private:
        ulong m_Term;
        ELogValueType m_eValueType;
        CPtr<CBuffer> m_pBuff;
    };
}
#endif //_LOG_ENTRY_HXX_