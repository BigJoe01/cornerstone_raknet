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

#ifndef _RESP_MSG_HXX_
#define _RESP_MSG_HXX_

namespace cornerstone {
    class CResponseMsg : public CMsgBase {
    public:
        CResponseMsg(ulong term, EMsgType type, int32 src, int32 dst, ulong next_idx = 0L, bool accepted = false)
            : CMsgBase(term, type, src, dst), m_ulNextIndex(next_idx), m_bAccepted(accepted) {}

        __nocopy__(CResponseMsg)

    public:
        ulong GetNextIndex() const {
            return m_ulNextIndex;
        }

        bool GetAccepted() const {
            return m_bAccepted;
        }

        void Accept(ulong next_idx) {
            m_ulNextIndex = next_idx;
            m_bAccepted = true;
        }
    private:
        ulong m_ulNextIndex;
        bool m_bAccepted;
    };
}

#endif //_RESP_MSG_HXX_