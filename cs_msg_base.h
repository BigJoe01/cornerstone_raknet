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

#ifndef _MSG_BASE_HXX_
#define _MSG_BASE_HXX_
namespace cornerstone {

    class CMsgBase {
    public:
        CMsgBase(ulong term, EMsgType type, int src, int dst)
            : m_Term(term), m_eType(type), m_iSrc(src), m_iDest(dst) {}

        virtual ~CMsgBase() {}

        ulong GetTerm() const {
            return this->m_Term;
        }

        EMsgType GetType() const {
            return this->m_eType;
        }

        int32 GetSrc() const {
            return this->m_iSrc;
        }

        int32 GetDest() const {
            return this->m_iDest;
        }

    __nocopy__(CMsgBase)

    private:
        ulong m_Term;
        EMsgType m_eType;
        int32 m_iSrc;
        int32 m_iDest;
    };
}
#endif //_MSG_BASE_HXX_