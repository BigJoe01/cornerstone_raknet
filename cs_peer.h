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

#ifndef _PEER_HXX_
#define _PEER_HXX_

namespace cornerstone {
    class CRaftPeer {
    public:
        CRaftPeer(const CServerConfig& config, const SRaftContext& ctx, CTimerTask<CRaftPeer&>::executor& hb_exec)
            : m_Config(config),
            m_Scheduler(ctx.m_Scheduler),
            m_Rpc(ctx.m_RpcCliFactory.CreateClient(config.GetEndpoint())),
            m_iCurrentHbInterval(ctx.m_Params->m_iHeartbeatInterval),
            m_iHbInterval(ctx.m_Params->m_iHeartbeatInterval),
            m_iRpcBackoff(ctx.m_Params->m_iRpcFailureBackoff),
            m_iMaxHbInterval(ctx.m_Params->MaxHbInterval()),
            m_ulNextLogIndex(0),
            m_ulMatchedIndex(0),
            m_bBusyFlag(false),
            m_bPendingCommitFlag(false),
            m_bHbEnabled(false),
            m_HbTask(cs_new<CTimerTask<CRaftPeer&>, CTimerTask<CRaftPeer&>::executor&, CRaftPeer&>(hb_exec, *this)),
            m_SnapshotSyncContext(),
            m_Mutex(){
        }

    __nocopy__(CRaftPeer)
    
    public:
        int32 get_id() const {
            return m_Config.GetId();
        }

        const CServerConfig& get_config() {
            return m_Config;
        }

        CPtr<CDelayedTask>& get_hb_task() {
            return m_HbTask;
        }

        std::mutex& get_lock() {
            return m_Mutex;
        }

        int32 get_current_hb_interval() const {
            return m_iCurrentHbInterval;
        }

        bool make_busy() {
            bool f = false;
            return m_bBusyFlag.compare_exchange_strong(f, true);
        }

        void set_free() {
            m_bBusyFlag.store(false);
        }

        bool is_hb_enabled() const {
            return m_bHbEnabled;
        }

        void enable_hb(bool enable) {
            m_bHbEnabled = enable;
            if (!enable) {
                m_Scheduler.Cancel(m_HbTask);
            }
        }

        ulong get_next_log_idx() const {
            return m_ulNextLogIndex;
        }

        void set_next_log_idx(ulong idx) {
            m_ulNextLogIndex = idx;
        }

        ulong get_matched_idx() const {
            return m_ulMatchedIndex;
        }

        void set_matched_idx(ulong idx) {
            m_ulMatchedIndex = idx;
        }

        void set_pending_commit() {
            m_bPendingCommitFlag.store(true);
        }

        bool clear_pending_commit() {
            bool t = true;
            return m_bPendingCommitFlag.compare_exchange_strong(t, false);
        }

        void set_snapshot_in_sync(const CPtr<CSnapshot>& s) {
            if (s == nilptr) {
                m_SnapshotSyncContext.reset();
            }
            else {
                m_SnapshotSyncContext = cs_new<CSnapshotSyncContext>(s);
            }
        }

        CPtr<CSnapshotSyncContext> get_snapshot_sync_ctx() const {
            return m_SnapshotSyncContext;
        }

        void slow_down_hb() {
            m_iCurrentHbInterval = std::min(m_iMaxHbInterval, m_iCurrentHbInterval + m_iRpcBackoff);
        }

        void resume_hb_speed() {
            m_iCurrentHbInterval = m_iHbInterval;
        }

        void send_req(CPtr<CRequestMessage>& req, rpc_handler& handler);
    private:
        void handle_rpc_result(CPtr<CRequestMessage>& req, CPtr<rpc_result>& pending_result, CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err);
    private:
        const CServerConfig& m_Config;
        CDelayedTaskScheduler& m_Scheduler;
        CPtr<CRpcClient> m_Rpc;
        int32 m_iCurrentHbInterval;
        int32 m_iHbInterval;
        int32 m_iRpcBackoff;
        int32 m_iMaxHbInterval;
        ulong m_ulNextLogIndex;
        ulong m_ulMatchedIndex;
        std::atomic_bool m_bBusyFlag;
        std::atomic_bool m_bPendingCommitFlag;
        bool m_bHbEnabled;
        CPtr<CDelayedTask> m_HbTask;
        CPtr<CSnapshotSyncContext> m_SnapshotSyncContext;
        std::mutex m_Mutex;
    };
}

#endif //_PEER_HXX_
