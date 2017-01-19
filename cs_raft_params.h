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

#ifndef _RAFT_PARAMS_HXX_
#define _RAFT_PARAMS_HXX_

namespace cornerstone {
    struct CRaftParams {
    public:
        CRaftParams()
            : m_iElectionTimeoutUpperBound(300),
            m_iElectionTimeoutLowerBound(150),
            m_iHeartbeatInterval(75),
            m_iRpcFailureBackoff(25),
            m_iLogSyncBatchSize_(1000),
            m_iLogSyncStopGap(10),
            m_iSnapshotSistance(0),
            m_iSnapshotBlockSize(0),
            m_iMaxAppendSize(100) {}

        __nocopy__(CRaftParams)
    public:
        /**
        * Election timeout upper bound in milliseconds
        * @param timeout
        * @return self
        */
        CRaftParams& WithElectionTimeoutUpper(int32 timeout) {
            m_iElectionTimeoutUpperBound = timeout;
            return *this;
        }

        /**
        * Election timeout lower bound in milliseconds
        * @param timeout
        * @return self
        */
        CRaftParams& WithElectionTimeoutLower(int32 timeout) {
            m_iElectionTimeoutLowerBound = timeout;
            return *this;
        }

        /**
        * heartbeat interval in milliseconds
        * @param hb_interval
        * @return self
        */
        CRaftParams& WithHbInterval(int32 hb_interval) {
            m_iHeartbeatInterval = hb_interval;
            return *this;
        }

        /**
        * Rpc failure backoff in milliseconds
        * @param backoff
        * @return self
        */
        CRaftParams& WithRpcFailureBackoff(int32 backoff) {
            m_iRpcFailureBackoff = backoff;
            return *this;
        }

        /**
        * The maximum log entries could be attached to an appendEntries call
        * @param size
        * @return self
        */
        CRaftParams& WithMaxAppendSize(int32 size) {
            m_iMaxAppendSize = size;
            return *this;
        }

        /**
        * For new member that just joined the cluster, we will use log sync to ask it to catch up,
        * and this parameter is to specify how many log entries to pack for each sync request
        * @param batch_size
        * @return self
        */
        CRaftParams& WithLogSyncBatchSize(int32 batch_size) {
            m_iLogSyncBatchSize_ = batch_size;
            return *this;
        }

        /**
        * For new member that just joined the cluster, we will use log sync to ask it to catch up,
        * and this parameter is to tell when to stop using log sync but appendEntries for the new server
        * when leaderCommitIndex - indexCaughtUp < logSyncStopGap, then appendEntries will be used
        * @param gap
        * @return self
        */
        CRaftParams& WithLogSyncStoppingGap(int32 gap) {
            m_iLogSyncStopGap = gap;
            return *this;
        }

        /**
        * Enable log compact and snapshot with the commit distance
        * @param commit_distance, log distance to compact between two snapshots
        * @return self
        */
        CRaftParams& WithSnapshotEnabled(int32 commit_distance) {
            m_iSnapshotSistance = commit_distance;
            return *this;
        }

        /**
        * The tcp block size for syncing the snapshots
        * @param size
        * @return self
        */
        CRaftParams& WithSnapshotSyncBlockSize(int32 size) {
            m_iSnapshotBlockSize = size;
            return *this;
        }

        int MaxHbInterval() const {
            return std::max(m_iHeartbeatInterval, m_iElectionTimeoutLowerBound - (m_iHeartbeatInterval / 2));
        }

    public:
        int32 m_iElectionTimeoutUpperBound;
        int32 m_iElectionTimeoutLowerBound;
        int32 m_iHeartbeatInterval;
        int32 m_iRpcFailureBackoff;
        int32 m_iLogSyncBatchSize_;
        int32 m_iLogSyncStopGap;
        int32 m_iSnapshotSistance;
        int32 m_iSnapshotBlockSize;
        int32 m_iMaxAppendSize;
    };
}

#endif //_RAFT_PARAMS_HXX_