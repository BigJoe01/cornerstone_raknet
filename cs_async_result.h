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

#ifndef _ASYNC_HXX_
#define _ASYNC_HXX_

namespace cornerstone {
    template<typename T, typename TE = CPtr<std::exception>>
    class CAsyncResult {
    public:
        typedef std::function<void(T&, TE&)> handler_type;
        CAsyncResult() : m_Error(), m_bHasResult(false), m_Mutex(), m_ConditionVariable() {}
        explicit CAsyncResult(T& result)
            : m_Result(result), m_Error(), m_bHasResult(true), m_Mutex(), m_ConditionVariable() {}
        explicit CAsyncResult(handler_type& handler)
            : m_Error(), m_bHasResult(true), m_Handler(handler), m_Mutex(), m_ConditionVariable() {}

        ~CAsyncResult() {}

        __nocopy__(CAsyncResult)

    public:
        void Ready(handler_type& handler) {
            std::lock_guard<std::mutex> guard(m_Mutex);
            if (m_bHasResult) {
                handler(m_Result, m_Error);
            }
            else {
                m_Handler = handler;
            }
        }

        void SetResult(T& result, TE& err) {
	        {
                std::lock_guard<std::mutex> guard(m_Mutex);
                m_Result = result;
                m_Error = err;
                m_bHasResult = true;
	            if (m_Handler) {
		            m_Handler(result, err);
		        }
	        }

            m_ConditionVariable.notify_all();
        }

        T& Get() {
            std::unique_lock<std::mutex> lock(m_Mutex);
            if (m_bHasResult) {
                if (m_Error == nullptr) {
                    return m_Result;
                }

                throw m_Error;
            }

            m_ConditionVariable.wait(lock);
            if (m_Error == nullptr) {
                return m_Result;
            }

            throw m_Error;
        }

    private:
        T m_Result;
        TE m_Error;
        bool m_bHasResult;
        handler_type m_Handler;
        std::mutex m_Mutex;
        std::condition_variable m_ConditionVariable;
    };
}

#endif //_ASYNC_HXX_
