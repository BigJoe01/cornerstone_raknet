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

#ifndef _RAFT_SERVER_HXX_
#define _RAFT_SERVER_HXX_

namespace cornerstone {
    class CRaftServer {
    public:
        CRaftServer(SContext* ctx);
        virtual ~CRaftServer();
	    __nocopy__(CRaftServer)
    
    public:
        CPtr<CResponseMsg> process_req(CRequestMessage& req);

        CPtr<CAsyncResult<bool>> add_srv(const CServerConfig& srv);

        CPtr<CAsyncResult<bool>> remove_srv(const int srv_id);

        CPtr<CAsyncResult<bool>> append_entries(const std::vector<CPtr<CBuffer>>& logs);

    private:
        typedef std::unordered_map<int32, CPtr<peer>>::const_iterator peer_itor;

    private:
        CPtr<CResponseMsg> handle_append_entries(CRequestMessage& req);
        CPtr<CResponseMsg> handle_vote_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_cli_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_extended_msg(CRequestMessage& req);
        CPtr<CResponseMsg> handle_install_snapshot_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_rm_srv_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_add_srv_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_log_sync_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_join_cluster_req(CRequestMessage& req);
        CPtr<CResponseMsg> handle_leave_cluster_req(CRequestMessage& req);
        bool handle_snapshot_sync_req(CSnapshotSyncRequest& req);
        void request_vote();
        void request_append_entries();
        bool request_append_entries(peer& p);
        void handle_peer_resp(CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err);
        void handle_append_entries_resp(CResponseMsg& resp);
        void handle_install_snapshot_resp(CResponseMsg& resp);
        void handle_voting_resp(CResponseMsg& resp);
        void handle_ext_resp(CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err);
        void handle_ext_resp_err(CRpcException& err);
        CPtr<CRequestMessage> create_append_entries_req(peer& p);
        CPtr<CRequestMessage> create_sync_snapshot_req(peer& p, ulong last_log_idx, ulong term, ulong commit_idx);
        void commit(ulong target_idx);
        void snapshot_and_compact(ulong committed_idx);
        bool update_term(ulong term);
        void reconfigure(const CPtr<CClusterConfig>& new_config);
        void become_leader();
        void become_follower();
        void enable_hb_for_peer(peer& p);
        void restart_election_timer();
        void stop_election_timer();
        void handle_hb_timeout(peer& peer);
        void handle_election_timeout();
        void sync_log_to_new_srv(ulong start_idx);
        void invite_srv_to_join_cluster();
        void rm_srv_from_cluster(int32 srv_id);
        int get_snapshot_sync_block_size() const;
        void on_snapshot_completed(CPtr<CSnapshot>& s, bool result, CPtr<std::exception>& err);
        void on_retryable_req_err(CPtr<peer>& p, CPtr<CRequestMessage>& req);
        ulong term_for_log(ulong log_idx);
        void commit_in_bg();
        CPtr<CAsyncResult<bool>> send_msg_to_leader(CPtr<CRequestMessage>& req);
    private:
        static const int default_snapshot_sync_block_size;
        int32 leader_;
        int32 id_;
        int32 votes_responded_;
        int32 votes_granted_;
        ulong quick_commit_idx_;
        bool election_completed_;
        bool config_changing_;
        bool catching_up_;
        bool stopping_;
        int32 steps_to_down_;
        std::atomic_bool snp_in_progress_;
        std::unique_ptr<SContext> ctx_;
        CDelayedTaskScheduler& scheduler_;
        CTimerTask<void>::executor election_exec_;
        CPtr<CDelayedTask> election_task_;
        std::unordered_map<int32, CPtr<peer>> peers_;
        std::unordered_map<int32, CPtr<CRpcClient>> rpc_clients_;
        EServerRole role_;
        CPtr<CServerState> state_;
        CPtr<CLogStore> log_store_;
        CStateMachine& state_machine_;
        CLogger& l_;
        std::function<int32()> rand_timeout_;
        CPtr<CClusterConfig> config_;
        CPtr<peer> srv_to_join_;
        CPtr<CServerConfig> conf_to_add_;
        std::recursive_mutex lock_;
        std::mutex commit_lock_;
        std::mutex rpc_clients_lock_;
        std::condition_variable commit_cv_;
        std::mutex stopping_lock_;
        std::condition_variable ready_to_stop_cv_;
        rpc_handler resp_handler_;
        rpc_handler ex_resp_handler_;
    };
}
#endif //_RAFT_SERVER_HXX_
