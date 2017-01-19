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

#ifndef _REG_MSG_HXX_
#define _REG_MSG_HXX_

namespace cornerstone {
    class CRequestMessage : public CMsgBase {
    public:
        CRequestMessage(ulong term, EMsgType type, int32 src, int32 dst, ulong last_log_term, ulong last_log_idx, ulong commit_idx)
            : CMsgBase(term, type, src, dst), m_ulLastLogTerm(last_log_term), m_ulLastLogIndex(last_log_idx), m_CommitIndex(commit_idx), m_LogEntries() {
        }
        
        virtual ~CRequestMessage() __override__ {
        }

    __nocopy__(CRequestMessage)

    public:
        ulong get_last_log_idx() const {
            return m_ulLastLogIndex;
        }

        ulong get_last_log_term() const {
            return m_ulLastLogTerm;
        }

        ulong get_commit_idx() const {
            return m_CommitIndex;
        }

        std::vector<CPtr<CLogEntry>>& log_entries() {
            return m_LogEntries;
        }

    private:
        ulong m_ulLastLogTerm;
        ulong m_ulLastLogIndex;
        ulong m_CommitIndex;
        std::vector<CPtr<CLogEntry>> m_LogEntries;
    };
}

#endif //_REG_MSG_HXX_