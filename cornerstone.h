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

#ifndef _CORNERSTONE_HXX_
#define _CORNERSTONE_HXX_

#include <cstdio>
#include <cstdlib>
//#include <cinttypes>

#include <cstring>
#include <memory>
#include <vector>
#include <list>
#include <string>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <exception>
#include <atomic>
#include <algorithm>
#include <unordered_map>
#include <random>
#include <chrono>
#include <thread>
#include <fstream>

#ifdef max
#undef max
#endif

#ifdef min
#undef min
#endif

#include "cornerstone/cs_pp_util.h"
#include "cornerstone/cs_strfmt.h"
#include "cornerstone/cs_types.h"
#include "cornerstone/cs_ptr.h"
#include "cornerstone/cs_raft_params.h"
#include "cornerstone/cs_msg_type.h"
#include "cornerstone/cs_buffer.h"
#include "cornerstone/cs_log_value_type.h"
#include "cornerstone/cs_log_entry.h"
#include "cornerstone/cs_msg_base.h"
#include "cornerstone/cs_req_msg.h"
#include "cornerstone/cs_resp_msg.h"
#include "cornerstone/cs_rpc_exception.h"
#include "cornerstone/cs_async_result.h"
#include "cornerstone/cs_log.h"
#include "cornerstone/cs_srv_config.h"
#include "cornerstone/cs_cluster_config.h"
#include "cornerstone/cs_srv_state.h"
#include "cornerstone/cs_srv_role.h"
#include "cornerstone/cs_log_store.h"
#include "cornerstone/cs_state_mgr.h"
#include "cornerstone/cs_rpc_listener.h"
#include "cornerstone/cs_snapshot.h"
#include "cornerstone/cs_state_machine.h"
#include "cornerstone/cs_rpc_cli.h"
#include "cornerstone/cs_rpc_cli_factory.h"
#include "cornerstone/cs_delayed_task.h"
#include "cornerstone/cs_timer_task.h"
#include "cornerstone/cs_task_scheduler.h"
#include "cornerstone/cs_context.h"
#include "cornerstone/cs_snapshot_sync_ctx.h"
#include "cornerstone/cs_snapshot_sync_req.h"
#include "cornerstone/cs_peer.h"
#include "cornerstone/cs_raft_server.h"
#include "cornerstone/cs_fs_log_store.h"
#endif // _CORNERSTONE_HXX_
