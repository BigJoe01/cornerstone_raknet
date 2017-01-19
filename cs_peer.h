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
    class peer {
    public:
        peer(const CServerConfig& config, const SContext& ctx, CTimerTask<peer&>::executor& hb_exec)
            : config_(config),
            scheduler_(ctx.m_Scheduler),
            rpc_(ctx.m_RpcCliFactory.CreateClient(config.GetEndpoint())),
            current_hb_interval_(ctx.m_Params->m_iHeartbeatInterval),
            hb_interval_(ctx.m_Params->m_iHeartbeatInterval),
            rpc_backoff_(ctx.m_Params->m_iRpcFailureBackoff),
            max_hb_interval_(ctx.m_Params->MaxHbInterval()),
            next_log_idx_(0),
            matched_idx_(0),
            busy_flag_(false),
            pending_commit_flag_(false),
            hb_enabled_(false),
            hb_task_(cs_new<CTimerTask<peer&>, CTimerTask<peer&>::executor&, peer&>(hb_exec, *this)),
            snp_sync_ctx_(),
            lock_(){
        }

    __nocopy__(peer)
    
    public:
        int32 get_id() const {
            return config_.GetId();
        }

        const CServerConfig& get_config() {
            return config_;
        }

        CPtr<CDelayedTask>& get_hb_task() {
            return hb_task_;
        }

        std::mutex& get_lock() {
            return lock_;
        }

        int32 get_current_hb_interval() const {
            return current_hb_interval_;
        }

        bool make_busy() {
            bool f = false;
            return busy_flag_.compare_exchange_strong(f, true);
        }

        void set_free() {
            busy_flag_.store(false);
        }

        bool is_hb_enabled() const {
            return hb_enabled_;
        }

        void enable_hb(bool enable) {
            hb_enabled_ = enable;
            if (!enable) {
                scheduler_.Cancel(hb_task_);
            }
        }

        ulong get_next_log_idx() const {
            return next_log_idx_;
        }

        void set_next_log_idx(ulong idx) {
            next_log_idx_ = idx;
        }

        ulong get_matched_idx() const {
            return matched_idx_;
        }

        void set_matched_idx(ulong idx) {
            matched_idx_ = idx;
        }

        void set_pending_commit() {
            pending_commit_flag_.store(true);
        }

        bool clear_pending_commit() {
            bool t = true;
            return pending_commit_flag_.compare_exchange_strong(t, false);
        }

        void set_snapshot_in_sync(const CPtr<CSnapshot>& s) {
            if (s == nilptr) {
                snp_sync_ctx_.reset();
            }
            else {
                snp_sync_ctx_ = cs_new<CSnapshotSyncContext>(s);
            }
        }

        CPtr<CSnapshotSyncContext> get_snapshot_sync_ctx() const {
            return snp_sync_ctx_;
        }

        void slow_down_hb() {
            current_hb_interval_ = std::min(max_hb_interval_, current_hb_interval_ + rpc_backoff_);
        }

        void resume_hb_speed() {
            current_hb_interval_ = hb_interval_;
        }

        void send_req(CPtr<CRequestMessage>& req, rpc_handler& handler);
    private:
        void handle_rpc_result(CPtr<CRequestMessage>& req, CPtr<rpc_result>& pending_result, CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err);
    private:
        const CServerConfig& config_;
        CDelayedTaskScheduler& scheduler_;
        CPtr<CRpcClient> rpc_;
        int32 current_hb_interval_;
        int32 hb_interval_;
        int32 rpc_backoff_;
        int32 max_hb_interval_;
        ulong next_log_idx_;
        ulong matched_idx_;
        std::atomic_bool busy_flag_;
        std::atomic_bool pending_commit_flag_;
        bool hb_enabled_;
        CPtr<CDelayedTask> hb_task_;
        CPtr<CSnapshotSyncContext> snp_sync_ctx_;
        std::mutex lock_;
    };
}

#endif //_PEER_HXX_
