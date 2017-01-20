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

using namespace cornerstone;

const int CRaftServer::default_snapshot_sync_block_size = 4 * 1024;

// for tracing and debugging
static const char* __msg_type_str[] = {
    "unknown",
    "request_vote_request",
    "request_vote_response",
    "append_entries_request",
    "append_entries_response",
    "client_request",
    "add_server_request",
    "add_server_response",
    "remove_server_request",
    "remove_server_response",
    "sync_log_request",
    "sync_log_response",
    "join_cluster_request",
    "join_cluster_response",
    "leave_cluster_request",
    "leave_cluster_response",
    "install_snapshot_request",
    "install_snapshot_response"
};

CPtr<CResponseMsg> CRaftServer::ProcessRequest(CRequestMessage& req) {
    recur_lock(lock_);
    l_.Debug(
        lstrfmt("Receive a %s message from %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu and Term=%llu")
        .fmt(
            __msg_type_str[req.GetType()],
            req.GetSrc(),
            req.get_last_log_idx(),
            req.get_last_log_term(),
            req.log_entries().size(),
            req.get_commit_idx(),
            req.GetTerm()));
    if (req.GetType() == EMsgType::append_entries_request ||
        req.GetType() == EMsgType::request_vote_request ||
        req.GetType() == EMsgType::install_snapshot_request) {
        // we allow the server to be continue after term updated to save a round message
        update_term(req.GetTerm());

        // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
        if (steps_to_down_ > 0) {
            steps_to_down_ = 2;
        }
    }

    CPtr<CResponseMsg> resp;
    if (req.GetType() == EMsgType::append_entries_request) {
        resp = handle_append_entries(req);
    }
    else if (req.GetType() == EMsgType::request_vote_request) {
        resp = handle_vote_req(req);
    }
    else if (req.GetType() == EMsgType::client_request) {
        resp = handle_cli_req(req);
    }
    else {
        // extended requests
        resp = handle_extended_msg(req);
    }

    if (resp) {
        l_.Debug(
            lstrfmt("Response back a %s message to %d with Accepted=%d, Term=%llu, NextIndex=%llu")
            .fmt(
                __msg_type_str[resp->GetType()],
                resp->GetDest(),
                resp->GetAccepted() ? 1 : 0,
                resp->GetTerm(),
                resp->GetNextIndex()));
    }

    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_append_entries(CRequestMessage& req) {
    if (req.GetTerm() == state_->GetTerm()) {
        if (role_ == EServerRole::candidate) {
            become_follower();
        }
        else if (role_ == EServerRole::leader) {
            l_.Debug(
                lstrfmt("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits")
                .fmt(req.GetSrc()));
            ctx_->m_StateManager.SystemExit(-1);
            ::exit(-1);
        }
        else {
            restart_election_timer();
        }
    }

    // After a snapshot the req.get_last_log_idx() may less than log_store_->next_slot() but equals to log_store_->next_slot() -1
    // In this case, log is Okay if req.get_last_log_idx() == lastSnapshot.get_last_log_idx() && req.get_last_log_term() == lastSnapshot.get_last_log_term()
    // In not accepted case, we will return log_store_->next_slot() for the leader to quick jump to the index that might aligned
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::append_entries_response, id_, req.GetSrc(), log_store_->NextSlot()));
    bool log_okay = req.get_last_log_idx() == 0 ||
        (req.get_last_log_idx() < log_store_->NextSlot() && req.get_last_log_term() == term_for_log(req.get_last_log_idx()));
    if (req.GetTerm() < state_->GetTerm() || !log_okay) {
        return resp;
    }

    // follower & log is okay
    if (req.log_entries().size() > 0) {
        // write logs to store, start from overlapped logs
        ulong idx = req.get_last_log_idx() + 1;
        size_t log_idx = 0;
        while (idx < log_store_->NextSlot() && log_idx < req.log_entries().size()) {
            if (log_store_->TermAt(idx) == req.log_entries().at(log_idx)->GetTerm()) {
                idx++;
                log_idx++;
            }
            else {
                break;
            }
        }

        // dealing with overwrites
        while (idx < log_store_->NextSlot() && log_idx < req.log_entries().size()) {
            CPtr<CLogEntry> old_entry(log_store_->EntryAt(idx));
            if (old_entry->GetType() == ELogValueType::e_Application) {
                state_machine_.Rollback(idx, old_entry->GetBuffer());
            }
            else if (old_entry->GetType() == ELogValueType::e_Configuration) {
                l_.Info(sstrfmt("revert from a prev config change to config at %llu").fmt(config_->GetLogIndex()));
                config_changing_ = false;
            }

            log_store_->WriteAt(idx++, req.log_entries().at(log_idx++));
        }

        // append new log entries
        while (log_idx < req.log_entries().size()) {
            CPtr<CLogEntry> entry = req.log_entries().at(log_idx ++);
            ulong idx_for_entry = log_store_->Append(entry);
            if (entry->GetType() == ELogValueType::e_Configuration) {
                l_.Info(sstrfmt("receive a config change from leader at %llu").fmt(idx_for_entry));
                config_changing_ = true;

            }
            else {
                state_machine_.PreCommit(idx_for_entry, entry->GetBuffer());
            }
        }
    }

    leader_ = req.GetSrc();
    commit(req.get_commit_idx());
    resp->Accept(req.get_last_log_idx() + req.log_entries().size() + 1);
    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_vote_req(CRequestMessage& req) {
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::request_vote_response, id_, req.GetSrc()));
    bool log_okay = req.get_last_log_term() > log_store_->LastEntry()->GetTerm() ||
        (req.get_last_log_term() == log_store_->LastEntry()->GetTerm() &&
            log_store_->NextSlot() - 1 <= req.get_last_log_idx());
    bool grant = req.GetTerm() == state_->GetTerm() && log_okay && (state_->GetVotedFor() == req.GetSrc() || state_->GetVotedFor() == -1);
    if (grant) {
        resp->Accept(log_store_->NextSlot());
        state_->SetVotedFor(req.GetSrc());
        ctx_->m_StateManager.SaveState(*state_);
    }

    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_cli_req(CRequestMessage& req) {
    CPtr<CResponseMsg> resp (cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::append_entries_response, id_, leader_));
    if (role_ != EServerRole::leader) {
        return resp;
    }

    std::vector<CPtr<CLogEntry>>& entries = req.log_entries();
    for (size_t i = 0; i < entries.size(); ++i) {
        log_store_->Append(entries.at(i));
        state_machine_.PreCommit(log_store_->NextSlot() - 1, entries.at(i)->GetBuffer());
    }

    // urgent commit, so that the commit will not depend on hb
    request_append_entries();
    resp->Accept(log_store_->NextSlot());
    return resp;
}

void CRaftServer::handle_election_timeout() {
    recur_lock(lock_);
    if (steps_to_down_ > 0) {
        if (--steps_to_down_ == 0) {
            l_.Info("no hearing further news from leader, remove this server from cluster and step down");
            for (std::list<CPtr<CServerConfig>>::iterator it = config_->GetServers().begin();
                it != config_->GetServers().end();
                ++it) {
                if ((*it)->GetId() == id_) {
                    config_->GetServers().erase(it);
                    ctx_->m_StateManager.SaveConfig(*config_);
                    break;
                }
            }

            ctx_->m_StateManager.SystemExit(-1);
            ::exit(0);
            return;
        }

        l_.Info(sstrfmt("stepping down (cycles left: %d), skip this election timeout event").fmt(steps_to_down_));
        restart_election_timer();
        return;
    }

    if (catching_up_) {
        // this is a new server for the cluster, will not send out vote req until conf that includes this srv is committed
        l_.Info("election timeout while joining the cluster, ignore it.");
        restart_election_timer();
        return;
    }

    if (role_ == EServerRole::leader) {
        l_.Error("A leader should never encounter election timeout, illegal application state, stop the application");
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        return;
    }

    l_.Debug("Election timeout, change to Candidate");
    state_->IncTerm();
    state_->SetVotedFor(-1);
    role_ = EServerRole::candidate;
    votes_granted_ = 0;
    votes_responded_ = 0;
    election_completed_ = false;
    ctx_->m_StateManager.SaveState(*state_);
    request_vote();

    // restart the election timer if this is not yet a leader
    if (role_ != EServerRole::leader) {
        restart_election_timer();
    }
}

void CRaftServer::request_vote() {
    l_.Info(sstrfmt("requestVote started with term %llu").fmt(state_->GetTerm()));
    state_->SetVotedFor(id_);
    ctx_->m_StateManager.SaveState(*state_);
    votes_granted_ += 1;
    votes_responded_ += 1;

    // is this the only server?
    if (votes_granted_ > (int32)(peers_.size() + 1) / 2) {
        election_completed_ = true;
        become_leader();
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        CPtr<CRequestMessage> req(cs_new<CRequestMessage>(state_->GetTerm(), EMsgType::request_vote_request, id_, it->second->get_id(), term_for_log(log_store_->NextSlot() - 1), log_store_->NextSlot() - 1, state_->GetCommitIndex()));
        l_.Debug(sstrfmt("send %s to server %d with term %llu").fmt(__msg_type_str[req->GetType()], it->second->get_id(), state_->GetTerm()));
        it->second->send_req(req, resp_handler_);
    }
}

void CRaftServer::request_append_entries() {
    if (peers_.size() == 0) {
        commit(log_store_->NextSlot() - 1);
        return;
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        request_append_entries(*it->second);
    }
}

bool CRaftServer::request_append_entries(CRaftPeer& p) {
    if (p.make_busy()) {
        CPtr<CRequestMessage> msg = create_append_entries_req(p);
        p.send_req(msg, resp_handler_);
        return true;
    }

    l_.Debug(sstrfmt("Server %d is busy, skip the request").fmt(p.get_id()));
    return false;
}

void CRaftServer::handle_peer_resp(CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err) {
    recur_lock(lock_);
    if (err) {
        l_.Info(sstrfmt("peer response error: %s").fmt(err->what()));
        return;
    }

    l_.Debug(
        lstrfmt("Receive a %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
        .fmt(__msg_type_str[resp->GetType()], resp->GetSrc(), resp->GetAccepted() ? 1 : 0, resp->GetTerm(), resp->GetNextIndex()));

    // if term is updated, no more action is required
    if (update_term(resp->GetTerm())) {
        return;
    }

    // ignore the response that with lower term for safety
    switch (resp->GetType())
    {
    case EMsgType::request_vote_response:
        handle_voting_resp(*resp);
        break;
    case EMsgType::append_entries_response:
        handle_append_entries_resp(*resp);
        break;
    case EMsgType::install_snapshot_response:
        handle_install_snapshot_resp(*resp);
        break;
    default:
        l_.Error(sstrfmt("Received an unexpected message %s for response, system exits.").fmt(__msg_type_str[resp->GetType()]));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        break;
    }
}

void CRaftServer::handle_append_entries_resp(CResponseMsg& resp) {
    peer_itor it = peers_.find(resp.GetSrc());
    if (it == peers_.end()) {
        l_.Info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.GetSrc()));
        return;
    }

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    CPtr<CRaftPeer> p = it->second;
    if (resp.GetAccepted()) {
        {
            std::lock_guard<std::mutex>(p->get_lock());
            p->set_next_log_idx(resp.GetNextIndex());
            p->set_matched_idx(resp.GetNextIndex() - 1);
        }

        // try to commit with this response
        // TODO: keep this to save a "new" operation for each response
        std::unique_ptr<ulong> matched_indexes(new ulong[peers_.size() + 1]);
        matched_indexes.get()[0] = log_store_->NextSlot() - 1;
        int i = 1;
        for (it = peers_.begin(); it != peers_.end(); ++it, ++i) {
            matched_indexes.get()[i] = it->second->get_matched_idx();
        }

        std::sort(matched_indexes.get(), matched_indexes.get() + (peers_.size() + 1), std::greater<ulong>());
        commit(matched_indexes.get()[(peers_.size() + 1) / 2]);
        need_to_catchup = p->clear_pending_commit() || resp.GetNextIndex() < log_store_->NextSlot();
    }
    else {
        std::lock_guard<std::mutex> guard(p->get_lock());
        if (resp.GetNextIndex() > 0 && p->get_next_log_idx() > resp.GetNextIndex()) {
            // fast move for the peer to catch up
            p->set_next_log_idx(resp.GetNextIndex());
        }
        else {
            p->set_next_log_idx(p->get_next_log_idx() - 1);
        }
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == EServerRole::leader && need_to_catchup) {
        request_append_entries(*p);
    }
}

void CRaftServer::handle_install_snapshot_resp(CResponseMsg& resp) {
    peer_itor it = peers_.find(resp.GetSrc());
    if (it == peers_.end()) {
        l_.Info(sstrfmt("the response is from an unkonw peer %d").fmt(resp.GetSrc()));
        return;
    }

    // if there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
    bool need_to_catchup = true;
    CPtr<CRaftPeer> p = it->second;
    if (resp.GetAccepted()) {
        std::lock_guard<std::mutex> guard(p->get_lock());
        CPtr<CSnapshotSyncContext> sync_ctx = p->get_snapshot_sync_ctx();
        if (sync_ctx == nilptr) {
            l_.Info("no snapshot sync context for this peer, drop the response");
            need_to_catchup = false;
        }
        else {
            if (resp.GetNextIndex() >= sync_ctx->GetSnapshot()->Size()) {
                l_.Debug("snapshot sync is done");
                CPtr<CSnapshot> nil_snp;
                p->set_next_log_idx(sync_ctx->GetSnapshot()->GetLastLogIndex() + 1);
                p->set_matched_idx(sync_ctx->GetSnapshot()->GetLastLogIndex());
                p->set_snapshot_in_sync(nil_snp);
                need_to_catchup = p->clear_pending_commit() || resp.GetNextIndex() < log_store_->NextSlot();
            }
            else {
                l_.Debug(sstrfmt("continue to sync snapshot at offset %llu").fmt(resp.GetNextIndex()));
                sync_ctx->SetOffset(resp.GetNextIndex());
            }
        }
    }
    else {
        l_.Info("peer declines to install the snapshot, will retry");
    }

    // This may not be a leader anymore, such as the response was sent out long time ago
    // and the role was updated by UpdateTerm call
    // Try to match up the logs for this peer
    if (role_ == EServerRole::leader && need_to_catchup) {
        request_append_entries(*p);
    }
}

void CRaftServer::handle_voting_resp(CResponseMsg& resp) {
    votes_responded_ += 1;
    if (election_completed_) {
        l_.Info("Election completed, will ignore the voting result from this server");
        return;
    }

    if (resp.GetAccepted()) {
        votes_granted_ += 1;
    }

    if (votes_responded_ >= (int32)(peers_.size() + 1)) {
        election_completed_ = true;
    }

    if (votes_granted_ > (int32)((peers_.size() + 1) / 2)) {
        l_.Info(sstrfmt("Server is elected as leader for term %llu").fmt(state_->GetTerm()));
        election_completed_ = true;
        become_leader();
    }
}

void CRaftServer::handle_hb_timeout(CRaftPeer& p) {
    recur_lock(lock_);
    l_.Debug(sstrfmt("Heartbeat timeout for %d").fmt(p.get_id()));
    if (role_ == EServerRole::leader) {
        request_append_entries(p);
        {
            std::lock_guard<std::mutex> guard(p.get_lock());
            if (p.is_hb_enabled()) {
                // Schedule another heartbeat if heartbeat is still enabled 
                ctx_->m_Scheduler.Schedule(p.get_hb_task(), p.get_current_hb_interval());
            }
            else {
                l_.Debug(sstrfmt("heartbeat is disabled for peer %d").fmt(p.get_id()));
            }
        }
    }
    else {
        l_.Info(sstrfmt("Receive a heartbeat event for %d while no longer as a leader").fmt(p.get_id()));
    }
}

void CRaftServer::restart_election_timer() {
    // don't start the election timer while this server is still catching up the logs
    if (catching_up_) {
        return;
    }

    if (election_task_) {
        scheduler_.Cancel(election_task_);
    }
    else {
        election_task_ = cs_new<CTimerTask<void>>(election_exec_);

    }

    scheduler_.Schedule(election_task_, rand_timeout_());
}

void CRaftServer::stop_election_timer() {
    if (!election_task_) {
        l_.Warning("Election Timer is never started but is requested to stop, protential a bug");
        return;
    }

    scheduler_.Cancel(election_task_);
}

void CRaftServer::become_leader() {
    stop_election_timer();
    role_ = EServerRole::leader;
    leader_ = id_;
    srv_to_join_.reset();
    CPtr<CSnapshot> nil_snp;
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        it->second->set_next_log_idx(log_store_->NextSlot());
        it->second->set_snapshot_in_sync(nil_snp);
        it->second->set_free();
        enable_hb_for_peer(*(it->second));
    }

    if (config_->GetLogIndex() == 0) {
        config_->SetLogIndex(log_store_->NextSlot());
        CPtr<CBuffer> conf_buf = config_->Serialize();
        CPtr<CLogEntry> entry(cs_new<CLogEntry>(state_->GetTerm(), conf_buf, ELogValueType::e_Configuration));
        log_store_->Append(entry);
        l_.Info("save initial config to log store");
        config_changing_ = true;
    }

    request_append_entries();
}

void CRaftServer::enable_hb_for_peer(CRaftPeer& p) {
    p.enable_hb(true);
    p.resume_hb_speed();
    scheduler_.Schedule(p.get_hb_task(), p.get_current_hb_interval());
}

void CRaftServer::become_follower() {
    // stop hb for all peers
    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        it->second->enable_hb(false);
    }

    srv_to_join_.reset();
    role_ = EServerRole::follower;
    restart_election_timer();
}

bool CRaftServer::update_term(ulong term) {
    if (term > state_->GetTerm()) {
        state_->SetTerm(term);
        state_->SetVotedFor(-1);
        election_completed_ = false;
        votes_granted_ = 0;
        votes_responded_ = 0;
        ctx_->m_StateManager.SaveState(*state_);
        become_follower();
        return true;
    }

    return false;
}

void CRaftServer::commit(ulong target_idx) {
    if (target_idx > quick_commit_idx_) {
        quick_commit_idx_ = target_idx;

        // if this is a leader notify peers to commit as well
        // for peers that are free, send the request, otherwise, set pending commit flag for that peer
        if (role_ == EServerRole::leader) {
            for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
                if (!request_append_entries(*(it->second))) {
                    it->second->set_pending_commit();
                }
            }
        }
    }

    if (log_store_->NextSlot() - 1 > state_->GetCommitIndex() && quick_commit_idx_ > state_->GetCommitIndex()) {
        commit_cv_.notify_one();
    }
}

void CRaftServer::snapshot_and_compact(ulong committed_idx) {
    bool snapshot_in_action = false;
    try {
        bool f = false;
        if (ctx_->m_Params->m_iSnapshotSistance > 0
            && (committed_idx - log_store_->StartIndex()) > (ulong)ctx_->m_Params->m_iSnapshotSistance
            && snp_in_progress_.compare_exchange_strong(f, true)) {
            snapshot_in_action = true;
            CPtr<CSnapshot> snp(state_machine_.LastSnapshot());
            if (snp && (committed_idx - snp->GetLastLogIndex()) < (ulong)ctx_->m_Params->m_iSnapshotSistance) {
                l_.Info(sstrfmt("a very recent snapshot is available at index %llu, will skip this one").fmt(snp->GetLastLogIndex()));
                snp_in_progress_.store(false);
                snapshot_in_action = false;
            }
            else {
                l_.Info(sstrfmt("creating a snapshot for index %llu").fmt(committed_idx));

                // get the latest configuration info
                CPtr<CClusterConfig> conf(config_);
                while (conf->GetLogIndex() > committed_idx && conf->GetPrevLogIndex() >= log_store_->StartIndex()) {
                    CPtr<CLogEntry> conf_log(log_store_->EntryAt(conf->GetPrevLogIndex()));
                    conf = CClusterConfig::Deserialize(conf_log->GetBuffer());
                }

                if (conf->GetLogIndex() > committed_idx &&
                    conf->GetPrevLogIndex() > 0 &&
                    conf->GetPrevLogIndex() < log_store_->StartIndex()) {
                    CPtr<CSnapshot> s(state_machine_.LastSnapshot());
                    if (!s) {
                        l_.Error("No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting");
                        ctx_->m_StateManager.SystemExit(-1);
                        ::exit(-1);
                        return;
                    }

                    conf = s->GetLastConfig();
                }
                else if (conf->GetLogIndex() > committed_idx && conf->GetPrevLogIndex() == 0) {
                    l_.Error("BUG!!! stop the system, there must be a configuration at index one");
                    ctx_->m_StateManager.SystemExit(-1);
                    ::exit(-1);
                    return;
                }

                ulong idx_to_compact = committed_idx - 1;
                ulong log_term_to_compact = log_store_->TermAt(idx_to_compact);
                CPtr<CSnapshot> new_snapshot(cs_new<CSnapshot>(idx_to_compact, log_term_to_compact, conf));
                CAsyncResult<bool>::handler_type handler = (CAsyncResult<bool>::handler_type) std::bind(&CRaftServer::on_snapshot_completed, this, new_snapshot, std::placeholders::_1, std::placeholders::_2);
                state_machine_.CreateSnapshot(
                    *new_snapshot,
                    handler);
                snapshot_in_action = false;
            }
        }
    }
    catch (...) {
        l_.Error(sstrfmt("failed to compact logs at index %llu due to errors").fmt(committed_idx));
        if (snapshot_in_action) {
            bool val = true;
            snp_in_progress_.compare_exchange_strong(val, false);
        }
    }
}

void CRaftServer::on_snapshot_completed(CPtr<CSnapshot>& s, bool result, CPtr<std::exception>& err) {
    do {
        if (err != nilptr) {
            l_.Error(lstrfmt("failed to create a snapshot due to %s").fmt(err->what()));
            break;
        }

        if (!result) {
            l_.Info("the state machine rejects to create the snapshot");
            break;
        }

        {
            recur_lock(lock_);
            l_.Debug("snapshot created, compact the log store");
            log_store_->Compact(s->GetLastLogIndex());
        }
    } while (false);
    snp_in_progress_.store(false);
}

CPtr<CRequestMessage> CRaftServer::create_append_entries_req(CRaftPeer& p) {
    ulong cur_nxt_idx(0L);
    ulong commit_idx(0L);
    ulong last_log_idx(0L);
    ulong term(0L);
    ulong starting_idx(1L);

    {
        recur_lock(lock_);
        starting_idx = log_store_->StartIndex();
        cur_nxt_idx = log_store_->NextSlot();
        commit_idx = quick_commit_idx_;
        term = state_->GetTerm();
    }

    {
        std::lock_guard<std::mutex> guard(p.get_lock());
        if (p.get_next_log_idx() == 0L) {
            p.set_next_log_idx(cur_nxt_idx);
        }

        last_log_idx = p.get_next_log_idx() - 1;
    }

    if (last_log_idx >= cur_nxt_idx) {
        l_.Error(sstrfmt("Peer's lastLogIndex is too large %llu v.s. %llu, server exits").fmt(last_log_idx, cur_nxt_idx));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        return CPtr<CRequestMessage>();
    }

    // for syncing the snapshots, for starting_idx - 1, we can check with last snapshot
    if (last_log_idx > 0 && last_log_idx < starting_idx - 1) {
        return create_sync_snapshot_req(p, last_log_idx, term, commit_idx);
    }

    ulong last_log_term = term_for_log(last_log_idx);
    ulong end_idx = std::min(cur_nxt_idx, last_log_idx + 1 + ctx_->m_Params->m_iMaxAppendSize);
    CPtr<std::vector<CPtr<CLogEntry>>> log_entries((last_log_idx + 1) >= cur_nxt_idx ? CPtr<std::vector<CPtr<CLogEntry>>>() : log_store_->LogEntries(last_log_idx + 1, end_idx));
    l_.Debug(
        lstrfmt("An AppendEntries Request for %d with LastLogIndex=%llu, LastLogTerm=%llu, EntriesLength=%d, CommitIndex=%llu and Term=%llu")
        .fmt(
            p.get_id(),
            last_log_idx,
            last_log_term,
            log_entries ? log_entries->size() : 0,
            commit_idx,
            term));
    CPtr<CRequestMessage> req(cs_new<CRequestMessage>(term, EMsgType::append_entries_request, id_, p.get_id(), last_log_term, last_log_idx, commit_idx));
    std::vector<CPtr<CLogEntry>>& v = req->log_entries();
    if (log_entries) {
        v.insert(v.end(), log_entries->begin(), log_entries->end());
    }

    return req;
}

void CRaftServer::reconfigure(const CPtr<CClusterConfig>& new_config) {
    l_.Debug(
        lstrfmt("system is reconfigured to have %d servers, last config index: %llu, this config index: %llu")
        .fmt(new_config->GetServers().size(), new_config->GetPrevLogIndex(), new_config->GetLogIndex()));

    // we only allow one server to be added or removed at a time
    std::vector<int32> srvs_removed;
    std::vector<CPtr<CServerConfig>> srvs_added;
    std::list<CPtr<CServerConfig>>& new_srvs(new_config->GetServers());
    for (std::list<CPtr<CServerConfig>>::const_iterator it = new_srvs.begin(); it != new_srvs.end(); ++it) {
        peer_itor pit = peers_.find((*it)->GetId());
        if (pit == peers_.end() && id_ != (*it)->GetId()) {
            srvs_added.push_back(*it);
        }
    }

    for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
        if (!new_config->GetServer(it->first)) {
            srvs_removed.push_back(it->first);
        }
    }

    if (!new_config->GetServer(id_)) {
        srvs_removed.push_back(id_);
    }

    for (std::vector<CPtr<CServerConfig>>::const_iterator it = srvs_added.begin(); it != srvs_added.end(); ++it) {
        CPtr<CServerConfig> srv_added(*it);
        CTimerTask<CRaftPeer&>::executor exec = (CTimerTask<CRaftPeer&>::executor)std::bind(&CRaftServer::handle_hb_timeout, this, std::placeholders::_1);
        CPtr<CRaftPeer> p = cs_new<CRaftPeer, CServerConfig&, SRaftContext&, CTimerTask<CRaftPeer&>::executor&>(*srv_added, *ctx_, exec);
        p->set_next_log_idx(log_store_->NextSlot());
        peers_.insert(std::make_pair(srv_added->GetId(), p));
        l_.Info(sstrfmt("server %d is added to cluster").fmt(srv_added->GetId()));
        if (role_ == EServerRole::leader) {
            l_.Info(sstrfmt("enable heartbeating for server %d").fmt(srv_added->GetId()));
            enable_hb_for_peer(*p);
            if (srv_to_join_ && srv_to_join_->get_id() == p->get_id()) {
                p->set_next_log_idx(srv_to_join_->get_next_log_idx());
                srv_to_join_.reset();
            }
        }
    }

    for (std::vector<int32>::const_iterator it = srvs_removed.begin(); it != srvs_removed.end(); ++it) {
        int32 srv_removed = *it;
        if (srv_removed == id_ && !catching_up_) {
            // this server is removed from cluster
            ctx_->m_StateManager.SaveConfig(*new_config);
            l_.Info("server has been removed, step down");
            ctx_->m_StateManager.SystemExit(0);
            return;
        }

        peer_itor pit = peers_.find(srv_removed);
        if (pit != peers_.end()) {
            pit->second->enable_hb(false);
            peers_.erase(pit);
            l_.Info(sstrfmt("server %d is removed from cluster").fmt(srv_removed));
        }
        else {
            l_.Info(sstrfmt("peer %d cannot be found, no action for removing").fmt(srv_removed));
        }
    }

    config_ = new_config;
}

CPtr<CResponseMsg> CRaftServer::handle_extended_msg(CRequestMessage& req) {
    switch (req.GetType())
    {
    case EMsgType::add_server_request:
        return handle_add_srv_req(req);
    case EMsgType::remove_server_request:
        return handle_rm_srv_req(req);
    case EMsgType::sync_log_request:
        return handle_log_sync_req(req);
    case EMsgType::join_cluster_request:
        return handle_join_cluster_req(req);
    case EMsgType::leave_cluster_request:
        return handle_leave_cluster_req(req);
    case EMsgType::install_snapshot_request:
        return handle_install_snapshot_req(req);
    default:
        l_.Error(sstrfmt("receive an unknown request %s, for safety, step down.").fmt(__msg_type_str[req.GetType()]));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        break;
    }

    return CPtr<CResponseMsg>();
}

CPtr<CResponseMsg> CRaftServer::handle_install_snapshot_req(CRequestMessage& req) {
    if (req.GetTerm() == state_->GetTerm() && !catching_up_) {
        if (role_ == EServerRole::candidate) {
            become_follower();
        }
        else if (role_ == EServerRole::leader) {
            l_.Error(lstrfmt("Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a bug, server exits").fmt(req.GetSrc()));
            ctx_->m_StateManager.SystemExit(-1);
            ::exit(-1);
            return CPtr<CResponseMsg>();
        }
        else {
            restart_election_timer();
        }
    }

    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::install_snapshot_response, id_, req.GetSrc()));
    if (!catching_up_ && req.GetTerm() < state_->GetTerm()) {
        l_.Info("received an install snapshot request which has lower term than this server, decline the request");
        return resp;
    }

    std::vector<CPtr<CLogEntry>>& entries(req.log_entries());
    if (entries.size() != 1 || entries[0]->GetType() != ELogValueType::e_SnapshotSyncRequest) {
        l_.Warning("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value");
        return resp;
    }

    CPtr<CSnapshotSyncRequest> sync_req(CSnapshotSyncRequest::Deserialize(entries[0]->GetBuffer()));
    if (sync_req->GetSnapshot().GetLastLogIndex() <= state_->GetCommitIndex()) {
        l_.Warning(sstrfmt("received a snapshot (%llu) that is older than current log store").fmt(sync_req->GetSnapshot().GetLastLogIndex()));
        return resp;
    }

    if (handle_snapshot_sync_req(*sync_req)) {
        resp->Accept(sync_req->GetOffset() + sync_req->GetData().size());
    }
    
    return resp;
}

bool CRaftServer::handle_snapshot_sync_req(CSnapshotSyncRequest& req) {
    try {
        state_machine_.SaveSnapshotData(req.GetSnapshot(), req.GetOffset(), req.GetData());
        if (req.IsDone()) {
            // Only follower will run this piece of code, but let's check it again
            if (role_ != EServerRole::follower) {
                l_.Error("bad server role for applying a snapshot, exit for debugging");
                ctx_->m_StateManager.SystemExit(-1);
                ::exit(-1);
            }

            l_.Debug("sucessfully receive a snapshot from leader");
            if (log_store_->Compact(req.GetSnapshot().GetLastLogIndex())) {
                // The state machine will not be able to commit anything before the snapshot is applied, so make this synchronously
                // with election timer stopped as usually applying a snapshot may take a very long time
                stop_election_timer();
                l_.Info("successfully compact the log store, will now ask the statemachine to apply the snapshot");
                if (!state_machine_.ApplySnapshot(req.GetSnapshot())) {
                    l_.Info("failed to apply the snapshot after log compacted, to ensure the safety, will shutdown the system");
                    ctx_->m_StateManager.SystemExit(-1);
                    ::exit(-1);
                    return false;
                }

                reconfigure(req.GetSnapshot().GetLastConfig());
                ctx_->m_StateManager.SaveConfig(*config_);
                state_->SetCommitIndex(req.GetSnapshot().GetLastLogIndex());
                quick_commit_idx_ = req.GetSnapshot().GetLastLogIndex();
                ctx_->m_StateManager.SaveState(*state_);
                restart_election_timer();
                l_.Info("snapshot is successfully applied");
            }
            else {
                l_.Error("failed to compact the log store after a snapshot is received, will ask the leader to retry");
                return false;
            }
        }
    }
    catch (...) {
        l_.Error("failed to handle snapshot installation due to system errors");
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        return false;
    }

    return true;
}

void CRaftServer::handle_ext_resp(CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err) {
    recur_lock(lock_);
    if (err) {
        handle_ext_resp_err(*err);
        return;
    }

    l_.Debug(
        lstrfmt("Receive an extended %s message from peer %d with Result=%d, Term=%llu, NextIndex=%llu")
        .fmt(
            __msg_type_str[resp->GetType()],
            resp->GetSrc(),
            resp->GetAccepted() ? 1 : 0,
            resp->GetTerm(),
            resp->GetNextIndex()));

    switch (resp->GetType())
    {
    case EMsgType::sync_log_response:
        if (srv_to_join_) {
            // we are reusing heartbeat interval value to indicate when to stop retry
            srv_to_join_->resume_hb_speed();
            srv_to_join_->set_next_log_idx(resp->GetNextIndex());
            srv_to_join_->set_matched_idx(resp->GetNextIndex() - 1);
            sync_log_to_new_srv(resp->GetNextIndex()); 
        }
        break;
    case EMsgType::join_cluster_response:
        if (srv_to_join_) {
            if (resp->GetAccepted()) {
                l_.Debug("new server confirms it will join, start syncing logs to it");
                sync_log_to_new_srv(1);
            }
            else {
                l_.Debug("new server cannot accept the invitation, give up");
            }
        }
        else {
            l_.Debug("no server to join, drop the message");
        }
        break;
    case EMsgType::leave_cluster_response:
        if (!resp->GetAccepted()) {
            l_.Debug("peer doesn't accept to stepping down, stop proceeding");
            return;
        }

        l_.Debug("peer accepted to stepping down, removing this server from cluster");
        rm_srv_from_cluster(resp->GetSrc());
        break;
    case EMsgType::install_snapshot_response:
        {
            if (!srv_to_join_) {
                l_.Info("no server to join, the response must be very old.");
                return;
            }

            if (!resp->GetAccepted()) {
                l_.Info("peer doesn't accept the snapshot installation request");
                return;
            }

            CPtr<CSnapshotSyncContext> sync_ctx = srv_to_join_->get_snapshot_sync_ctx();
            if (sync_ctx == nilptr) {
                l_.Error("Bug! SnapshotSyncContext must not be null");
                ctx_->m_StateManager.SystemExit(-1);
                ::exit(-1);
                return;
            }

            if (resp->GetNextIndex() >= sync_ctx->GetSnapshot()->Size()) {
                // snapshot is done
                CPtr<CSnapshot> nil_snap;
                l_.Debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
                srv_to_join_->set_snapshot_in_sync(nil_snap);
                srv_to_join_->set_next_log_idx(sync_ctx->GetSnapshot()->GetLastLogIndex() + 1);
                srv_to_join_->set_matched_idx(sync_ctx->GetSnapshot()->GetLastLogIndex());
            }
            else {
                sync_ctx->SetOffset(resp->GetNextIndex());
                l_.Debug(sstrfmt("continue to send snapshot to new server at offset %llu").fmt(resp->GetNextIndex()));
            }

            sync_log_to_new_srv(srv_to_join_->get_next_log_idx());
        }
        break;
    default:
        l_.Error(lstrfmt("received an unexpected response message type %s, for safety, stepping down").fmt(__msg_type_str[resp->GetType()]));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        break;
    }
}

void CRaftServer::handle_ext_resp_err(CRpcException& err) {
    l_.Debug(lstrfmt("receive an rpc error response from peer server, %s").fmt(err.what()));
    CPtr<CRequestMessage> req = err.Request();
    if (req->GetType() == EMsgType::sync_log_request ||
        req->GetType() == EMsgType::join_cluster_request ||
        req->GetType() == EMsgType::leave_cluster_request) {
        CPtr<CRaftPeer> p;
        if (req->GetType() == EMsgType::leave_cluster_request) {
            peer_itor pit = peers_.find(req->GetDest());
            if (pit != peers_.end()) {
                p = pit->second;
            }
        }
        else {
            p = srv_to_join_;
        }

        if (p != nilptr) {
            if (p->get_current_hb_interval() >= ctx_->m_Params->MaxHbInterval()) {
                if (req->GetType() == EMsgType::leave_cluster_request) {
                    l_.Info(lstrfmt("rpc failed again for the removing server (%d), will remove this server directly").fmt(p->get_id()));

                    /**
                    * In case of there are only two servers in the cluster, it safe to remove the server directly from peers
                    * as at most one config change could happen at a time
                    *  prove:
                    *      assume there could be two config changes at a time
                    *      this means there must be a leader after previous leader offline, which is impossible
                    *      (no leader could be elected after one server goes offline in case of only two servers in a cluster)
                    * so the bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
                    * does not apply to cluster which only has two members
                    */
                    if (peers_.size() == 1) {
                        peer_itor pit = peers_.find(p->get_id());
                        if (pit != peers_.end()) {
                            pit->second->enable_hb(false);
                            peers_.erase(pit);
                            l_.Info(sstrfmt("server %d is removed from cluster").fmt(p->get_id()));
                        }
                        else {
                            l_.Info(sstrfmt("peer %d cannot be found, no action for removing").fmt(p->get_id()));
                        }
                    }

                    rm_srv_from_cluster(p->get_id());
                }
                else {
                    l_.Info(lstrfmt("rpc failed again for the new coming server (%d), will stop retry for this server").fmt(p->get_id()));
                    config_changing_ = false;
                    srv_to_join_.reset();
                }
            }
            else {
                // reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
                l_.Debug("retry the request");
                p->slow_down_hb();
                CTimerTask<void>::executor exec = (CTimerTask<void>::executor)std::bind(&CRaftServer::on_retryable_req_err, this, p, req);
                CPtr<CDelayedTask> task(cs_new<CTimerTask<void>>(exec));
                scheduler_.Schedule(task, p->get_current_hb_interval());
            }
        }
    }
}

void CRaftServer::on_retryable_req_err(CPtr<CRaftPeer>& p, CPtr<CRequestMessage>& req) {
    l_.Debug(sstrfmt("retry the request %s for %d").fmt(__msg_type_str[req->GetType()], p->get_id()));
    p->send_req(req, ex_resp_handler_);
}

CPtr<CResponseMsg> CRaftServer::handle_rm_srv_req(CRequestMessage& req) {
    std::vector<CPtr<CLogEntry>>& entries(req.log_entries());
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::remove_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->GetBuffer().size() != sz_int) {
        l_.Info("bad remove server request as we are expecting one log entry with value type of int");
        return resp;
    }

    if (role_ != EServerRole::leader) {
        l_.Info("this is not a leader, cannot handle RemoveServerRequest");
        return resp;
    }

    if (config_changing_) {
        // the previous config has not committed yet
        l_.Info("previous config has not committed yet");
        return resp;
    }

    int32 srv_id = entries[0]->GetBuffer().GetInt();
    if (srv_id == id_) {
        l_.Info("cannot request to remove leader");
        return resp;
    }

    peer_itor pit = peers_.find(srv_id);
    if (pit == peers_.end()) {
        l_.Info(sstrfmt("server %d does not exist").fmt(srv_id));
        return resp;
    }

    CPtr<CRaftPeer> p = pit->second;
    CPtr<CRequestMessage> leave_req(cs_new<CRequestMessage>(state_->GetTerm(), EMsgType::leave_cluster_request, id_, srv_id, 0, log_store_->NextSlot() - 1, quick_commit_idx_));
    p->send_req(leave_req, ex_resp_handler_);
    resp->Accept(log_store_->NextSlot());
    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_add_srv_req(CRequestMessage& req) {
    std::vector<CPtr<CLogEntry>>& entries(req.log_entries());
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::add_server_response, id_, leader_));
    if (entries.size() != 1 || entries[0]->GetType() != ELogValueType::e_ClusterServer) {
        l_.Debug("bad add server request as we are expecting one log entry with value type of ClusterServer");
        return resp;
    }

    if (role_ != EServerRole::leader) {
        l_.Info("this is not a leader, cannot handle AddServerRequest");
        return resp;
    }

    CPtr<CServerConfig> srv_conf(CServerConfig::Deserialize(entries[0]->GetBuffer()));
    if (peers_.find(srv_conf->GetId()) != peers_.end() || id_ == srv_conf->GetId()) {
        l_.Warning(lstrfmt("the server to be added has a duplicated id with existing server %d").fmt(srv_conf->GetId()));
        return resp;
    }

    if (config_changing_) {
        // the previous config has not committed yet
        l_.Info("previous config has not committed yet");
        return resp;
    }

    conf_to_add_ = std::move(srv_conf);
    CTimerTask<CRaftPeer&>::executor exec = (CTimerTask<CRaftPeer&>::executor)std::bind(&CRaftServer::handle_hb_timeout, this, std::placeholders::_1);
    srv_to_join_ = cs_new<CRaftPeer, CServerConfig&, SRaftContext&, CTimerTask<CRaftPeer&>::executor&>(*conf_to_add_, *ctx_, exec);
    invite_srv_to_join_cluster();
    resp->Accept(log_store_->NextSlot());
    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_log_sync_req(CRequestMessage& req) {
    std::vector<CPtr<CLogEntry>>& entries = req.log_entries();
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::sync_log_response, id_, req.GetSrc()));
    if (entries.size() != 1 || entries[0]->GetType() != ELogValueType::e_LogPack) {
        l_.Info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (!catching_up_) {
        l_.Info("This server is ready for cluster, ignore the request");
        return resp;
    }

    log_store_->ApplyPack(req.get_last_log_idx() + 1, entries[0]->GetBuffer());
    commit(log_store_->NextSlot() - 1);
    resp->Accept(log_store_->NextSlot());
    return resp;
}

void CRaftServer::sync_log_to_new_srv(ulong start_idx) {
    // only sync committed logs
    int32 gap = (int32)(quick_commit_idx_ - start_idx);
    if (gap < ctx_->m_Params->m_iLogSyncStopGap) {
        l_.Info(lstrfmt("LogSync is done for server %d with log gap %d, now put the server into cluster").fmt(srv_to_join_->get_id(), gap));
        CPtr<CClusterConfig> new_conf = cs_new<CClusterConfig>(log_store_->NextSlot(), config_->GetLogIndex());
        new_conf->GetServers().insert(new_conf->GetServers().end(), config_->GetServers().begin(), config_->GetServers().end());
        new_conf->GetServers().push_back(conf_to_add_);
        CPtr<CBuffer> new_conf_buf(new_conf->Serialize());
        CPtr<CLogEntry> entry(cs_new<CLogEntry>(state_->GetTerm(), new_conf_buf, ELogValueType::e_Configuration));
        log_store_->Append(entry);
        config_changing_ = true;
        request_append_entries();
        return;
    }

    CPtr<CRequestMessage> req;
    if (start_idx > 0 && start_idx < log_store_->StartIndex()) {
        req = create_sync_snapshot_req(*srv_to_join_, start_idx, state_->GetTerm(), quick_commit_idx_);
    }
    else {
        int32 size_to_sync = std::min(gap, ctx_->m_Params->m_iLogSyncBatchSize_);
        CPtr<CBuffer> log_pack = log_store_->Pack(start_idx, size_to_sync);
        req = cs_new<CRequestMessage>(state_->GetTerm(), EMsgType::sync_log_request, id_, srv_to_join_->get_id(), 0L, start_idx - 1, quick_commit_idx_);
        req->log_entries().push_back(cs_new<CLogEntry>(state_->GetTerm(), log_pack, ELogValueType::e_LogPack));
    }

    srv_to_join_->send_req(req, ex_resp_handler_);
}

void CRaftServer::invite_srv_to_join_cluster() {
    CPtr<CRequestMessage> req(cs_new<CRequestMessage>(state_->GetTerm(), EMsgType::join_cluster_request, id_, srv_to_join_->get_id(), 0L, log_store_->NextSlot() - 1, quick_commit_idx_));
    req->log_entries().push_back(cs_new<CLogEntry>(state_->GetTerm(), config_->Serialize(), ELogValueType::e_Configuration));
    srv_to_join_->send_req(req, ex_resp_handler_);
}

CPtr<CResponseMsg> CRaftServer::handle_join_cluster_req(CRequestMessage& req) {
    std::vector<CPtr<CLogEntry>>& entries = req.log_entries();
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::join_cluster_response, id_, req.GetSrc()));
    if (entries.size() != 1 || entries[0]->GetType() != ELogValueType::e_Configuration) {
        l_.Info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements");
        return resp;
    }

    if (catching_up_) {
        l_.Info("this server is already in log syncing mode");
        return resp;
    }

    catching_up_ = true;
    role_ = EServerRole::follower;
    leader_ = req.GetSrc();
    state_->SetCommitIndex(0);
    quick_commit_idx_ = 0;
    state_->SetVotedFor(-1);
    state_->SetTerm(req.GetTerm());
    ctx_->m_StateManager.SaveState(*state_);
    reconfigure(CClusterConfig::Deserialize(entries[0]->GetBuffer()));
    resp->Accept(log_store_->NextSlot());
    return resp;
}

CPtr<CResponseMsg> CRaftServer::handle_leave_cluster_req(CRequestMessage& req) {
    CPtr<CResponseMsg> resp(cs_new<CResponseMsg>(state_->GetTerm(), EMsgType::leave_cluster_response, id_, req.GetSrc()));
    if (!config_changing_) {
        steps_to_down_ = 2;
        resp->Accept(log_store_->NextSlot());
    }

    return resp;
}

void CRaftServer::rm_srv_from_cluster(int32 srv_id) {
    CPtr<CClusterConfig> new_conf = cs_new<CClusterConfig>(log_store_->NextSlot(), config_->GetLogIndex());
    for (CClusterConfig::const_srv_itor it = config_->GetServers().begin(); it != config_->GetServers().end(); ++it) {
        if ((*it)->GetId() != srv_id) {
            new_conf->GetServers().push_back(*it);
        }
    }

    l_.Info(lstrfmt("removed a server from configuration and save the configuration to log store at %llu").fmt(new_conf->GetLogIndex()));
    config_changing_ = true;
    CPtr<CBuffer> new_conf_buf(new_conf->Serialize());
    CPtr<CLogEntry> entry(cs_new<CLogEntry>(state_->GetTerm(), new_conf_buf, ELogValueType::e_Configuration));
    log_store_->Append(entry);
    request_append_entries();
}

int32 CRaftServer::get_snapshot_sync_block_size() const {
    int32 block_size = ctx_->m_Params->m_iSnapshotBlockSize;
    return block_size == 0 ? default_snapshot_sync_block_size : block_size;
}

CPtr<CRequestMessage> CRaftServer::create_sync_snapshot_req(CRaftPeer& p, ulong last_log_idx, ulong term, ulong commit_idx) {
    std::lock_guard<std::mutex> guard(p.get_lock());
    CPtr<CSnapshotSyncContext> sync_ctx = p.get_snapshot_sync_ctx();
    CPtr<CSnapshot> snp;
    if (sync_ctx != nilptr) {
        snp = sync_ctx->GetSnapshot();
    }

    CPtr<CSnapshot> last_snp(state_machine_.LastSnapshot());
    if (!snp || (last_snp && last_snp->GetLastLogIndex() > snp->GetLastLogIndex())) {
        snp = last_snp;
        if (snp == nilptr || last_log_idx > snp->GetLastLogIndex()) {
            l_.Error(
                lstrfmt("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot null: %d, snapshot doesn't contais lastLogIndex: %d")
                .fmt(p.get_id(), snp == nilptr ? 1 : 0, last_log_idx > snp->GetLastLogIndex() ? 1 : 0));
            ctx_->m_StateManager.SystemExit(-1);
            ::exit(-1);
            return CPtr<CRequestMessage>();
        }

        if (snp->Size() < 1L) {
            l_.Error("invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors");
            ctx_->m_StateManager.SystemExit(-1);
            ::exit(-1);
            return CPtr<CRequestMessage>();
        }

        l_.Info(sstrfmt("trying to sync snapshot with last index %llu to peer %d").fmt(snp->GetLastLogIndex(), p.get_id()));
        p.set_snapshot_in_sync(snp);
    }

    ulong offset = p.get_snapshot_sync_ctx()->GetOffset();
    int32 sz_left = (int32)(snp->Size() - offset);
    int32 blk_sz = get_snapshot_sync_block_size();
    CPtr<CBuffer> data = CBuffer::alloc((size_t)(std::min(blk_sz, sz_left)));
    int32 sz_rd = state_machine_.ReadSnapshotData(*snp, offset, *data);
    if ((size_t)sz_rd < data->size()) {
        l_.Error(lstrfmt("only %d bytes could be read from snapshot while %d bytes are expected, must be something wrong, exit.").fmt(sz_rd, data->size()));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
        return CPtr<CRequestMessage>();
    }

    std::unique_ptr<CSnapshotSyncRequest> sync_req(new CSnapshotSyncRequest(snp, offset, data, (offset + (ulong)data->size()) >= snp->Size()));
    CPtr<CRequestMessage> req(cs_new<CRequestMessage>(term, EMsgType::install_snapshot_request, id_, p.get_id(), snp->GetLastLogTerm(), snp->GetLastLogIndex(), commit_idx));
    req->log_entries().push_back(cs_new<CLogEntry>(term, sync_req->Serialize(), ELogValueType::e_SnapshotSyncRequest));
    return req;
}

ulong CRaftServer::term_for_log(ulong log_idx) {
    if (log_idx == 0) {
        return 0L;
    }

    if (log_idx >= log_store_->StartIndex()) {
        return log_store_->TermAt(log_idx);
    }

    CPtr<CSnapshot> last_snapshot(state_machine_.LastSnapshot());
    if (!last_snapshot || log_idx != last_snapshot->GetLastLogIndex()) {
        l_.Error(sstrfmt("bad log_idx %llu for retrieving the term value, kill the system to protect the system").fmt(log_idx));
        ctx_->m_StateManager.SystemExit(-1);
        ::exit(-1);
    }

    return last_snapshot->GetLastLogTerm();
}

void CRaftServer::commit_in_bg() {
    while (true) {
        try {
            ulong current_commit_idx = state_->GetCommitIndex();
            while (quick_commit_idx_ <= current_commit_idx
                || current_commit_idx >= log_store_->NextSlot() - 1) {
                std::unique_lock<std::mutex> lock(commit_lock_);
                commit_cv_.wait(lock);
                if (stopping_) {
                    lock.unlock();
                    lock.release();
                    {
                        auto_lock(stopping_lock_);
                        ready_to_stop_cv_.notify_all();
                    }

                    return;
                }

                current_commit_idx = state_->GetCommitIndex();
            }

            while (current_commit_idx < quick_commit_idx_ && current_commit_idx < log_store_->NextSlot() - 1) {
                current_commit_idx += 1;
                CPtr<CLogEntry> log_entry(log_store_->EntryAt(current_commit_idx));
                if (log_entry->GetType() == ELogValueType::e_Application) {
                    state_machine_.Commit(current_commit_idx, log_entry->GetBuffer());
                } else if (log_entry->GetType() == ELogValueType::e_Configuration) {
                    recur_lock(lock_);
                    log_entry->GetBuffer().Pos(0);
                    CPtr<CClusterConfig> new_conf = CClusterConfig::Deserialize(log_entry->GetBuffer());
                    l_.Info(sstrfmt("config at index %llu is committed").fmt(new_conf->GetLogIndex()));
                    ctx_->m_StateManager.SaveConfig(*new_conf);
                    config_changing_ = false;
                    if (config_->GetLogIndex() < new_conf->GetLogIndex()) {
                        reconfigure(new_conf);
                    }

                    if (catching_up_ && new_conf->GetServer(id_) != nilptr) {
                        l_.Info("this server is committed as one of cluster members");
                        catching_up_ = false;
                    }
                }

                state_->SetCommitIndex(current_commit_idx);
                snapshot_and_compact(current_commit_idx);
            }

            ctx_->m_StateManager.SaveState(*state_);
        }catch(std::exception& err){
            l_.Error(lstrfmt("background committing thread encounter err %s, exiting to protect the system").fmt(err.what()));
            ctx_->m_StateManager.SystemExit(-1);
            ::exit(-1);
        }
    }
}


CPtr<CAsyncResult<bool>> CRaftServer::AddServer(const CServerConfig& srv) {
    CPtr<CBuffer> buf(srv.Serialize());
    CPtr<CLogEntry> log(cs_new<CLogEntry>(0, buf, ELogValueType::e_ClusterServer));
    CPtr<CRequestMessage> req(cs_new<CRequestMessage>((ulong)0, EMsgType::add_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

CPtr<CAsyncResult<bool>> CRaftServer::AppendEntries(const std::vector<CPtr<CBuffer>>& logs) {
    if (logs.size() == 0) {
        bool result(false);
        return cs_new<CAsyncResult<bool>>(result);
    }

    CPtr<CRequestMessage> req(cs_new<CRequestMessage>((ulong)0, EMsgType::client_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    for (std::vector<CPtr<CBuffer>>::const_iterator it = logs.begin(); it != logs.end(); ++it) {
        CPtr<CLogEntry> log(cs_new<CLogEntry>(0, *it, ELogValueType::e_Application));
        req->log_entries().push_back(log);
    }

    return send_msg_to_leader(req);
}

CPtr<CAsyncResult<bool>> CRaftServer::RemoveServer(const int srv_id) {
    CPtr<CBuffer> buf(CBuffer::alloc(sz_int));
    buf->Put(srv_id);
    buf->Pos(0);
    CPtr<CLogEntry> log(cs_new<CLogEntry>(0, buf, ELogValueType::e_ClusterServer));
    CPtr<CRequestMessage> req(cs_new<CRequestMessage>((ulong)0, EMsgType::remove_server_request, 0, 0, (ulong)0, (ulong)0, (ulong)0));
    req->log_entries().push_back(log);
    return send_msg_to_leader(req);
}

CPtr<CAsyncResult<bool>> CRaftServer::send_msg_to_leader(CPtr<CRequestMessage>& req) {
    typedef std::unordered_map<int32, CPtr<CRpcClient>>::const_iterator rpc_client_itor;
    int32 leader_id = leader_;
    CPtr<CClusterConfig> cluster = config_;
    bool result(false);
    if (leader_id == -1) {
        return cs_new<CAsyncResult<bool>>(result);
    }

    if (leader_id == id_) {
        CPtr<CResponseMsg> resp = ProcessRequest(*req);
        result = resp->GetAccepted();
        return cs_new<CAsyncResult<bool>>(result);
    }

    CPtr<CRpcClient> rpc_cli;
    {
        auto_lock(rpc_clients_lock_);
        rpc_client_itor itor = rpc_clients_.find(leader_id);
        if (itor == rpc_clients_.end()) {
            CPtr<CServerConfig> srv_conf = config_->GetServer(leader_id);
            if (!srv_conf) {
                return cs_new<CAsyncResult<bool>>(result);
            }

            rpc_cli = ctx_->m_RpcCliFactory.CreateClient(srv_conf->GetEndpoint());
            rpc_clients_.insert(std::make_pair(leader_id, rpc_cli));
        }else{
            rpc_cli = itor->second;
        }
    }

    if (!rpc_cli) {
        return cs_new<CAsyncResult<bool>>(result);
    }

    CPtr<CAsyncResult<bool>> presult(cs_new<CAsyncResult<bool>>());
    rpc_handler handler = [presult](CPtr<CResponseMsg>& resp, CPtr<CRpcException>& err) -> void {
        bool rpc_success(false);
        CPtr<std::exception> perr;
        if (err) {
            perr = err;
        }
        else {
            rpc_success = resp && resp->GetAccepted();
        }

        presult->SetResult(rpc_success, perr);
    };
    rpc_cli->Send(req, handler);
    return presult;
}

CRaftServer::CRaftServer(SRaftContext* ctx)
        : leader_(-1),
            id_(ctx->m_StateManager.ServerId()),
            votes_responded_(0),
            votes_granted_(0),
            quick_commit_idx_(0),
            election_completed_(true),
            config_changing_(false),
            catching_up_(false),
            stopping_(false),
            steps_to_down_(0),
            snp_in_progress_(),
            ctx_(ctx),
            scheduler_(ctx->m_Scheduler),
            election_exec_(std::bind(&CRaftServer::handle_election_timeout, this)),
            election_task_(),
            peers_(),
            rpc_clients_(),
            role_(EServerRole::follower),
            state_(ctx->m_StateManager.ReadState()),
            log_store_(ctx->m_StateManager.LoadLogStore()),
            state_machine_(ctx->m_StateMachine),
            l_(ctx->m_Logger),
            config_(ctx->m_StateManager.LoadConfig()),
            srv_to_join_(),
            conf_to_add_(),
            lock_(),
            commit_lock_(),
            rpc_clients_lock_(),
            commit_cv_(),
            stopping_lock_(),
            ready_to_stop_cv_(),
            resp_handler_((rpc_handler)std::bind(&CRaftServer::handle_peer_resp, this, std::placeholders::_1, std::placeholders::_2)),
            ex_resp_handler_((rpc_handler)std::bind(&CRaftServer::handle_ext_resp, this, std::placeholders::_1, std::placeholders::_2)){
            uint seed = (uint)(std::chrono::system_clock::now().time_since_epoch().count() * id_);
            std::default_random_engine engine(seed);
            std::uniform_int_distribution<int32> distribution(ctx->m_Params->m_iElectionTimeoutLowerBound, ctx->m_Params->m_iElectionTimeoutUpperBound);
            rand_timeout_ = std::bind(distribution, engine);
            if (!state_) {
                state_ = cs_new<CServerState>();
                state_->SetCommitIndex(0);
                state_->SetTerm(0);
                state_->SetVotedFor(-1);
            }

            /**
            * I found this implementation is also a victim of bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
            * As the implementation is based on Diego's thesis
            * Fix:
            * We should never load configurations that is not committed,
            *   this prevents an old server from replicating an obsoleted config to other servers
            * The prove is as below:
            * Assume S0 is the last committed server set for the old server A
            * |- EXITS Log l which has been committed but l !BELONGS TO A.logs =>  Vote(A) < Majority(S0)
            * In other words, we need to prove that A cannot be elected to leader if any logs/configs has been committed.
            * Case #1, There is no configuration change since S0, then it's obvious that Vote(A) < Majority(S0), see the core Algorithm
            * Case #2, There are one or more configuration changes since S0, then at the time of first configuration change was committed,
            *      there are at least Majority(S0 - 1) servers committed the configuration change
            *      Majority(S0 - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
            * -|
            */
            for (ulong i = std::max(state_->GetCommitIndex() + 1, log_store_->StartIndex()); i < log_store_->NextSlot(); ++i) {
                CPtr<CLogEntry> entry(log_store_->EntryAt(i));
                if (entry->GetType() == ELogValueType::e_Configuration) {
                    l_.Info(sstrfmt("detect a configuration change that is not committed yet at index %llu").fmt(i));
                    config_changing_ = true;
                    break;
                }
            }

            std::list<CPtr<CServerConfig>>& srvs(config_->GetServers());
            for (CClusterConfig::srv_itor it = srvs.begin(); it != srvs.end(); ++it) {
                if ((*it)->GetId() != id_) {
         	        CTimerTask<CRaftPeer&>::executor exec = (CTimerTask<CRaftPeer&>::executor)std::bind(&CRaftServer::handle_hb_timeout, this, std::placeholders::_1);
                    peers_.insert(std::make_pair((*it)->GetId(), cs_new<CRaftPeer, CServerConfig&, SRaftContext&, CTimerTask<CRaftPeer&>::executor&>(**it, *ctx_, exec)));
                }
            }

            quick_commit_idx_ = state_->GetCommitIndex();
            std::thread commiting_thread = std::thread(std::bind(&CRaftServer::commit_in_bg, this));
            commiting_thread.detach();
            restart_election_timer();
            l_.Debug(strfmt<30>("server %d started").fmt(id_));
        }

CRaftServer::~CRaftServer() {
	recur_lock(lock_);
	stopping_ = true;
	std::unique_lock<std::mutex> commit_lock(commit_lock_);
	commit_cv_.notify_all();
	std::unique_lock<std::mutex> lock(stopping_lock_);
	commit_lock.unlock();
	commit_lock.release();
	ready_to_stop_cv_.wait(lock);
	if (election_task_) {
		scheduler_.Cancel(election_task_);
	}

	for (peer_itor it = peers_.begin(); it != peers_.end(); ++it) {
		if (it->second->get_hb_task()) {
			scheduler_.Cancel(it->second->get_hb_task());
		}
	}
}