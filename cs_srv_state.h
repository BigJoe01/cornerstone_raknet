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

#ifndef _SRV_STATE_HXX_
#define _SRV_STATE_HXX_

namespace cornerstone {
    class CServerState {
    public:
        CServerState()
            : m_ulTerm(0L), m_ulCommitIndex(0L), m_iVotedFor(-1) {}

        __nocopy__(CServerState)

    public:
        ulong GetTerm() const { return m_ulTerm; }
        void  SetTerm(ulong term) { m_ulTerm = term; }
        ulong GetCommitIndex() const { return m_ulCommitIndex; }
        void  SetCommitIndex(ulong commit_idx) {
            if (commit_idx > m_ulCommitIndex) {
                m_ulCommitIndex = commit_idx;
            }
        }

        int  GetVotedFor() const { return m_iVotedFor; }
        void SetVotedFor(int voted_for) { m_iVotedFor = voted_for; }
        void IncTerm() { m_ulTerm += 1; }
    private:
        ulong m_ulTerm;
        ulong m_ulCommitIndex;
        int m_iVotedFor;
    };
}

#endif
